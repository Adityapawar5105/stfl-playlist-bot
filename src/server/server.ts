import {once} from 'node:events'
import type {IncomingMessage, ServerResponse} from 'node:http'
import {context, reddit, redis, scheduler, settings} from '@devvit/web/server'
import type {
  PartialJsonValue,
  T3,
  TriggerResponse,
  UiResponse,
} from '@devvit/web/shared'

import {
  Endpoint,
  EndpointMethod,
  type ErrorRsp,
  type GetCounterRsp,
  type IncCounterReq,
  type IncCounterRsp,
} from '../shared/api.ts'
import {dbGetCounter, dbIncCounter} from './db.ts'

type AnyRsp =
  | GetCounterRsp
  | IncCounterRsp
  | UiResponse
  | TriggerResponse
  | ErrorRsp

const SPOTIFY_CLIENT_ID = 'c998fd639f554fbbb10cf113aef5a1d0'

const COMMENT_THRESHOLD = 100
const ACTIVE_WINDOW_MS = 72 * 60 * 60 * 1000
const TITLE_FALLBACK_DELAY_MS = 6 * 60 * 60 * 1000

const STATE_HASH = 'playlist-post-state'
const PROCESSED_HASH = 'playlist-processed-comments'
const PENDING_HASH = 'playlist-pending-comments'
const QUALIFIED_HASH = 'playlist-qualified-posts' // ONLY posts with 100+ comments
const CACHE_HASH = 'playlist-lookup-cache'
const META_HASH = 'playlist-meta'
const COMMENT_COUNT_HASH = 'playlist-comment-count'
const COMMENT_SEEN_HASH = 'playlist-comment-seen'
const PENDING_ORDER_HASH = 'playlist-pending-order'
const LEGACY_ACTIVE_HASH = 'playlist-active-posts'
const LEGACY_CLEANUP_HASH = 'playlist-cleanup'

// Tuning knobs. Outbound calls/min is roughly
//   MAX_HTTP_CALLS_PER_TICK * 60s / (tick duration + TICK_INTERVAL_MS)
// If you still see "too many requests", lower MAX_HTTP_CALLS_PER_TICK or
// raise HTTP_PACE_MS / TICK_INTERVAL_MS.
const TICK_INTERVAL_MS = 20 * 1000
const IDLE_TICK_INTERVAL_MS = 2 * 60 * 1000
const TICK_BUDGET_MS = 20 * 1000

// Deliberately conservative. The worker may become slower under extreme load,
// but it keeps queue state durable instead of hammering Spotify. Calls are
// spaced ~2s apart and a tick sends at most a handful, so traffic is a steady
// trickle instead of 12-call bursts.
const MAX_HTTP_CALLS_PER_TICK = 6
const HTTP_PACE_MS = 2000
const CREATION_CALLS_PER_TICK = 4
// Approx. seconds from one tick start to the next; used to spread the hourly
// allowance evenly across ticks.
const TICK_CYCLE_SECONDS = 30

// Hourly call governor. Spreads Spotify calls evenly so the bot keeps
// working around the clock instead of bursting and then sitting blocked.
// It lowers itself after every block and creeps back up while things are
// clean, so it settles on whatever Devvit actually allows.
// The floor is high enough that a backlog can still drain inside 72h; a stored
// allowance below it (e.g. one that collapsed under the old 30% cuts) resets
// to START. Back-offs are handled by the breaker pause, not by starving the bot.
const HOURLY_ALLOWANCE_START = 600
const HOURLY_ALLOWANCE_MIN = 120
const HOURLY_ALLOWANCE_MAX = 1500
const MAX_BACKOFF_MS = 15 * 60 * 1000
// Trips inside this window count as ONE incident for allowance reduction.
const TRIP_COOLDOWN_MS = 10 * 60 * 1000
const ALLOWANCE_GROW_AFTER_MS = 30 * 60 * 1000
// One cheap listing call (a few pages) every few minutes finds posts that reached
// 100 comments even if a comment trigger was dropped. Set to 0 to disable.
const SWEEP_INTERVAL_MS = 3 * 60 * 1000
const SWEEP_POST_LIMIT = 250
const CALLS_HASH = 'playlist-call-log'
const LOCK_TTL_MS = 5 * 60 * 1000

// Self-healing watchdog + playlist retention.
const WATCHDOG_INTERVAL_MS = 5 * 60 * 1000

const GENERIC_PLAYLIST_TITLE = 'Songs That Feel Like This'

type PlaylistState = {
  postId: string
  postTitle: string
  postAuthorName: string
  subredditName?: string
  createdAt: number
  activeUntil: number
  playlistId?: string
  playlistUrl?: string
  replyCommentId?: string
  trackUris: string[]
  created: boolean
  awaitingTitle: boolean
  fallbackApplied: boolean
  thresholdReached?: boolean
  backfilled?: boolean
  rescanned?: boolean
}

type TriggerEvent = {
  type?: string
  post?: {
    id?: string
  }
  comment?: {
    id?: string
    postId?: string
    parentId?: string
    author?: string
    body?: string
  }
}

type SchedulerEvent = {
  data?: {
    postId?: string
    commentId?: string
    playlistId?: string
    action?:
      | 'tick'
      | 'process'
      | 'title-fallback'
      | 'playlist-cleanup'
      | 'watchdog'
    gen?: string
  }
}

type Recommendation =
  | {
      kind: 'spotifyUrl'
      url: string
    }
  | {
      kind: 'youtubeUrl'
      url: string
    }
  | {
      kind: 'search'
      song: string
      artist: string
    }
  | {
      kind: 'searchText'
      text: string
    }

type PendingComment = {
  id: string
  body: string
  createdAt: number
}

async function getSpotifyAccessToken(refreshToken: string): Promise<string> {
  const response = await guardedFetch(
    'https://accounts.spotify.com/api/token',
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: new URLSearchParams({
        grant_type: 'refresh_token',
        refresh_token: refreshToken,
        client_id: SPOTIFY_CLIENT_ID,
      }),
    },
  )

  const data = (await response.json()) as {
    access_token?: string
    error?: string
    error_description?: string
  }

  if (!response.ok || !data.access_token) {
    throw new Error(
      `Spotify token refresh failed: ${
        data.error_description ?? data.error ?? 'Unknown error'
      }`,
    )
  }

  return data.access_token
}

function normalizeText(value: string): string {
  return value
    .toLowerCase()
    .replace(/[’']/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
}

async function spotifyFetch(
  url: string | URL,
  init: RequestInit,
): Promise<Response> {
  // No sleeping retry loop here any more: a 30s sleep x 3 inside one
  // request handler is what runs into the platform timeout. Rate limits
  // throw RateLimitError and the tick backs off as a whole.
  return guardedFetch(url, init)
}

async function spotifySearch(
  accessToken: string,
  query: string,
): Promise<
  Array<{
    uri: string
    name: string
    artists: Array<{name: string}>
  }>
> {
  const url = new URL('https://api.spotify.com/v1/search')
  url.search = new URLSearchParams({
    q: query,
    type: 'track',
    limit: '10',
  }).toString()

  const response = await spotifyFetch(url, {
    headers: {
      Authorization: `Bearer ${accessToken}`,
    },
  })

  if (!response.ok) {
    throw new Error(
      `Spotify search failed (${response.status}): ${await response.text()}`,
    )
  }

  const data = (await response.json()) as {
    tracks?: {
      items?: Array<{
        uri: string
        name: string
        artists: Array<{name: string}>
      }>
    }
  }

  return data.tracks?.items ?? []
}

type SpotifyTrack = {
  uri: string
  name: string
  artists: Array<{name: string}>
}

const VARIANT_WORDS =
  /\b(live|remix|karaoke|cover|tribute|instrumental|acoustic|sped up|slowed|reverb|8d|nightcore|lullaby|piano version)\b/i

// "Take It Easy - 2013 Remaster" / "Song (feat. X)" -> "Take It Easy" / "Song"
function cleanTrackTitle(name: string): string {
  return name
    .replace(
      /\s*[-–—]\s*(?:\d{4}\s+)?(?:remaster(?:ed)?|mono|stereo|single version|radio edit|album version|deluxe)\b.*$/i,
      '',
    )
    .replace(/\s*[([](?:feat|ft|with|remaster|\d{4})[^)\]]*[)\]]/gi, '')
    .trim()
}

function tokens(value: string): string[] {
  return normalizeText(value).split(' ').filter(Boolean)
}

// Strict: whole-word overlap (Jaccard). Unlike similarity(), "Easy" does
// NOT match "Take It Easy" just because one contains the other.
function titleScore(a: string, b: string): number {
  const left = new Set(tokens(a))
  const right = new Set(tokens(b))
  if (left.size === 0 || right.size === 0) return 0
  const shared = [...left].filter(word => right.has(word)).length
  return shared / (left.size + right.size - shared)
}

function artistScore(wanted: string, artists: Array<{name: string}>): number {
  const wantedTokens = tokens(wanted)
  if (wantedTokens.length === 0) return 0

  let best = 0

  for (const artist of artists) {
    const have = tokens(artist.name)
    if (have.length === 0) continue

    const shared = wantedTokens.filter(word => have.includes(word)).length
    const jaccard = shared / (wantedTokens.length + have.length - shared)
    const contained =
      shared === Math.min(wantedTokens.length, have.length) && shared > 0

    best = Math.max(best, jaccard, contained ? 0.85 : 0)
  }

  return best
}

function scoreCandidate(
  track: SpotifyTrack,
  song: string,
  artist: string,
): number {
  const songPart = Math.max(
    titleScore(cleanTrackTitle(track.name), song),
    titleScore(track.name, song),
  )
  const artistPart = artistScore(artist, track.artists)

  if (songPart < 0.8 || artistPart < 0.7) return 0

  const penalty =
    VARIANT_WORDS.test(track.name) && !VARIANT_WORDS.test(song) ? 0.15 : 0

  return 0.6 * songPart + 0.4 * artistPart - penalty
}

// Never trusts tracks[0]. Every candidate is scored on title AND artist,
// in both "Song - Artist" and "Artist - Song" orientation, and a track is
// only accepted if it is a strong match. Otherwise: skip it.
async function searchSpotifyTrack(
  accessToken: string,
  song: string,
  artist: string,
): Promise<string | null> {
  const clean = (value: string): string => value.replace(/["]/g, ' ').trim()

  const queries = [
    `track:"${clean(song)}" artist:"${clean(artist)}"`,
    `${clean(song)} ${clean(artist)}`,
  ]

  let best: {track: SpotifyTrack; score: number} | null = null

  for (const query of queries) {
    const candidates = await spotifySearch(accessToken, query)

    for (const track of candidates) {
      const score = Math.max(
        scoreCandidate(track, song, artist),
        scoreCandidate(track, artist, song),
      )

      if (score > 0 && (!best || score > best.score)) {
        best = {track, score}
      }
    }

    // Any confident match: no need to spend the second call. This roughly
    // halves Spotify search traffic.
    if (best && best.score >= 0.8) break
  }

  if (!best || best.score < 0.8) {
    console.log(`⏭️ No confident match for "${song}" / "${artist}"; skipping`)
    return null
  }

  console.log(
    `🎵 Spotify match: ${best.track.name} — ${best.track.artists
      .map(item => item.name)
      .join(', ')} (score ${best.score.toFixed(2)})`,
  )

  return best.track.uri
}

async function searchSpotifyText(
  accessToken: string,
  text: string,
): Promise<string | null> {
  const wanted = tokens(text)
  if (wanted.length === 0) return null

  const wantedSet = new Set(wanted)
  const candidates = await spotifySearch(accessToken, text)
  const wantsVariant = VARIANT_WORDS.test(text)

  type Scored = {track: SpotifyTrack; coverage: number; hasArtist: boolean}
  const scored: Scored[] = []

  for (const track of candidates) {
    if (!wantsVariant && VARIANT_WORDS.test(track.name)) continue

    const nameTokens = tokens(cleanTrackTitle(track.name))
    const artistTokens = tokens(track.artists.map(item => item.name).join(' '))
    const all = new Set([...nameTokens, ...artistTokens])

    const coverage = wanted.filter(word => all.has(word)).length / wanted.length
    const nameInText = nameTokens.every(word => wantedSet.has(word))
    const hasArtist = wanted.some(
      word => artistTokens.includes(word) && !nameTokens.includes(word),
    )

    if (nameInText && coverage >= 0.8) {
      scored.push({track, coverage, hasArtist})
    }
  }

  // Title AND artist both appear in the text: best coverage wins.
  const withArtist = scored
    .filter(item => item.hasArtist)
    .sort((a, b) => b.coverage - a.coverage)

  let pick: Scored | undefined = withArtist[0]

  // Title only ("take it easy"): ambiguous unless exactly one candidate
  // has that exact title. Otherwise skip rather than guess.
  if (!pick) {
    const exact = scored.filter(
      item =>
        tokens(cleanTrackTitle(item.track.name)).join(' ') === wanted.join(' '),
    )
    const distinct = new Set(exact.map(item => item.track.artists[0]?.name))
    if (exact.length > 0 && distinct.size === 1) pick = exact[0]
  }

  if (!pick) {
    console.log(`⏭️ No confident match for "${text}"; skipping`)
    return null
  }

  console.log(
    `🔎 Spotify text match: ${pick.track.name} — ${pick.track.artists
      .map(item => item.name)
      .join(', ')}`,
  )

  return pick.track.uri
}

async function spotifyTrackFromUrl(
  accessToken: string,
  url: string,
): Promise<string | null> {
  const match = url.match(
    /https?:\/\/open\.spotify\.com\/(?:intl-[^/]+\/)?track\/([A-Za-z0-9]+)/i,
  )

  if (!match?.[1]) {
    return null
  }

  const response = await spotifyFetch(
    `https://api.spotify.com/v1/tracks/${match[1]}`,
    {
      headers: {
        Authorization: `Bearer ${accessToken}`,
      },
    },
  )

  if (!response.ok) {
    console.log(`⚠️ Spotify link lookup failed (${response.status})`)

    return null
  }

  const track = (await response.json()) as {
    uri?: string
    name?: string
    artists?: Array<{
      name: string
    }>
  }

  if (!track.uri) {
    return null
  }

  console.log(
    `🔗 Spotify link match: ${track.name ?? 'Unknown'} — ${
      track.artists?.map(artist => artist.name).join(', ') ?? 'Unknown artist'
    }`,
  )

  return track.uri
}

async function youtubeDetails(
  url: string,
): Promise<{song: string; artist: string} | null> {
  // oEmbed 400s on URLs with extra params / music.youtube.com / playlists,
  // so rebuild a clean watch URL from the video id first.
  const canonical = canonicalYoutubeUrl(url)

  if (!canonical) {
    return null
  }

  const endpoint = new URL('https://www.youtube.com/oembed')

  endpoint.search = new URLSearchParams({
    url: canonical,
    format: 'json',
  }).toString()

  const response = await guardedFetch(endpoint)

  if (!response.ok) {
    console.log(`⚠️ YouTube metadata lookup failed (${response.status})`)

    return null
  }

  const data = (await response.json()) as {
    title?: string
    author_name?: string
  }

  const song = data.title?.trim()

  const artist = data.author_name
    ?.replace(/\s*-\s*Topic$/i, '')
    .replace(/\s*VEVO$/i, '')
    .trim()

  if (!song || !artist) {
    return null
  }

  console.log(`▶️ YouTube match: ${song} — ${artist}`)

  return {
    song,
    artist,
  }
}

function extractRecommendations(body: string): Recommendation[] {
  const results: Recommendation[] = []
  const seen = new Set<string>()

  const add = (item: Recommendation): void => {
    const key = JSON.stringify(item)
    if (!seen.has(key)) {
      seen.add(key)
      results.push(item)
    }
  }

  const spotifyUrls =
    body.match(
      /https?:\/\/open\.spotify\.com\/(?:intl-[^/\s]+\/)?track\/[A-Za-z0-9]+(?:\?[^\s)]*)?/gi,
    ) ?? []

  for (const url of spotifyUrls) add({kind: 'spotifyUrl', url})

  const youtubeUrls =
    body.match(
      /https?:\/\/(?:www\.)?(?:youtube\.com\/watch\?[^\s)]+|youtu\.be\/[A-Za-z0-9_-]+(?:\?[^\s)]*)?|music\.youtube\.com\/watch\?[^\s)]+)/gi,
    ) ?? []

  for (const url of youtubeUrls) add({kind: 'youtubeUrl', url})

  const lines = body
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean)

  const stripPrefix = (value: string): string =>
    value
      .replace(/^(?:[-•*]|\d+[.)])\s*/, '')
      .replace(/^['"“”]+|['"“”]+$/g, '')
      .trim()

  const cleanSong = (value: string): string =>
    value
      .replace(/^(?:maybe\.\.\.\s*)/i, '')
      .replace(/^(?:i\s+always\s+thought\s+)/i, '')
      .replace(/^(?:i\s+know\s+the\s+end\s+is\s+)/i, '')
      .replace(/^(?:the\s+entire\s+)/i, '')
      .replace(/^(?:the\s+song\s+)/i, '')
      .replace(/^['"“”]+|['"“”]+$/g, '')
      .trim()

  for (const originalLine of lines) {
    if (/https?:\/\//i.test(originalLine)) continue

    const line = stripPrefix(originalLine)

    // Group context such as "All by Ghost!" is handled by carrying the
    // artist onto subsequent plain song lines.
    if (/^all\s+by\s+/i.test(line)) continue

    // Album/context sentence such as "Ashnikko's entire WEEDKILLER album
    // but especially World Eater and WEEDKILLER (the song)". Extract the
    // explicitly named songs, not the entire album.
    const especially = line.match(
      /^(.+?)'s\s+entire\s+.+?album\s+but\s+especially\s+(.+)$/i,
    )
    if (especially?.[1] && especially[2]) {
      const artist = especially[1].trim()
      const songs = especially[2]
        .replace(/\([^)]*\)/g, '')
        .split(/\s+and\s+|,\s*/i)
        .map(song => song.trim())
        .filter(song => song.length >= 2)

      for (const song of songs) {
        add({kind: 'search', song, artist})
      }
      continue
    }

    // Quoted or embedded "Song by Artist" / "Song from Artist".
    const byMatches = [
      ...line.matchAll(
        /['"“”]([^'"“”]{2,100})['"“”]\s+by\s+([^,.;!?\n]{2,100})/gi,
      ),
    ]
    if (byMatches.length > 0) {
      for (const match of byMatches) {
        add({
          kind: 'search',
          song: cleanSong(match[1] ?? ''),
          artist: (match[2] ?? '').trim(),
        })
      }
      continue
    }

    const byIndex = line.search(/\s+by\s+/i)
    if (byIndex >= 0) {
      const match = line.match(/^(.+?)\s+by\s+(.+?)(?:[.!?](?:\s|$)|$)/i)
      if (match) {
        const song = cleanSong(match[1] ?? '')
        const artist = (match[2] ?? '')
          .replace(/,?\s+(?:even though|although|but|which|because)\b.*$/i, '')
          .trim()
        if (
          song.length >= 2 &&
          artist.length >= 2 &&
          song.length <= 100 &&
          artist.length <= 100
        ) {
          add({kind: 'search', song, artist})
          continue
        }
      }
    }

    // Handle both Song - Artist and Artist - Song, including a recommendation
    // embedded in a sentence. The Spotify resolver tests both orientations.
    const separator = line.match(/\s+[-—–]\s+/)
    if (separator?.index !== undefined) {
      const left = cleanSong(line.slice(0, separator.index))
      const right = line
        .slice(separator.index + separator[0].length)
        .replace(
          /\s+(?:sounded|sounds|is|was|felt|feels|because|even though)\b.*$/i,
          '',
        )
        .trim()

      if (
        left.length >= 2 &&
        right.length >= 2 &&
        left.length <= 100 &&
        right.length <= 100
      ) {
        add({kind: 'search', song: left, artist: right})
        continue
      }
    }

    // Plain short recommendation: search the whole phrase and let Spotify
    // validate it. Avoid long conversational prose.
    const plain = line.replace(/^['"“”]+|['"“”]+$/g, '').trim()

    if (
      plain.length >= 3 &&
      plain.length <= 80 &&
      !/^(?:i|we|you|this|that|maybe|not sure|honestly|also|another|the entire)\b/i.test(
        plain,
      ) &&
      !/[.!?]{2,}/.test(plain)
    ) {
      add({kind: 'searchText', text: plain})
    }
  }

  // Explicit "All by Artist" context: attach that artist to plain song lines.
  const allBy = body.match(/(?:^|\n)\s*all\s+by\s+([^!\n]+)!?\s*$/im)
  if (allBy?.[1]) {
    const artist = allBy[1].trim()
    for (const line of lines) {
      const clean = stripPrefix(line)
      if (
        clean &&
        !/\bby\b/i.test(clean) &&
        !/https?:\/\//i.test(clean) &&
        !/^all\s+by\b/i.test(clean) &&
        !/[-—–]/.test(clean) &&
        clean.length <= 100
      ) {
        add({kind: 'search', song: clean, artist})
      }
    }
  }

  return results
}

function isGenericPlaylistTitle(title: string): boolean {
  const normalized = title
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .replace(/\\s+/g, ' ')

  return normalized === GENERIC_PLAYLIST_TITLE.toLowerCase()
}

function makeTemporaryPlaylistTitle(postId: string): string {
  const shortId = postId.replace(/^t3_/, '').slice(-8)

  return `STFL • ${shortId}`
}

function makeFallbackPlaylistTitle(createdAt: number, postId: string): string {
  const date = new Date(createdAt).toISOString().slice(0, 10)

  const shortId = postId.replace(/^t3_/, '').slice(-8)

  return `Songs That Feel Like This • ${date} • ${shortId}`
}

function cleanCustomPlaylistTitle(body: string): string | null {
  const title = body.replace(/^[>\s]+/, '').trim()

  if (!title) {
    return null
  }

  if (title.length < 2 || title.length > 100) {
    return null
  }

  return title
}

async function renameSpotifyPlaylist(
  accessToken: string,
  playlistId: string,
  name: string,
): Promise<void> {
  const response = await spotifyFetch(
    `https://api.spotify.com/v1/playlists/${playlistId}`,
    {
      method: 'PUT',
      headers: {
        Authorization: `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        name: name.slice(0, 100),
      }),
    },
  )

  if (!response.ok) {
    throw new Error(
      `Spotify playlist rename failed (${response.status}): ${await response.text()}`,
    )
  }

  console.log(`✏️ Spotify playlist renamed to: ${name.slice(0, 100)}`)
}

// ─────────────────────────────────────────────────────────────────────────
// Global worker ("tick") design
//
// ONE scheduler job chain serves every post. Each tick:
//   1. takes a lease lock (auto-expires, so a crashed tick can never wedge it)
//   2. respects a global circuit breaker (paused after any rate-limit error)
//   3. spends a fixed HTTP-call budget, round-robin across active posts
//   4. flushes one "add tracks" call per post
// Comment triggers only write to Redis and make sure a tick is queued.
// ─────────────────────────────────────────────────────────────────────────

// Thrown for HTTP 429, Devvit's own "too many requests" error, and 5xx.
// Never swallowed: the tick stops, comments stay queued, breaker trips.
class RateLimitError extends Error {
  retryAfterMs?: number

  constructor(message: string, retryAfterMs?: number) {
    super(message)
    this.name = 'RateLimitError'
    this.retryAfterMs = retryAfterMs
  }
}

// Thrown when this tick has used its HTTP budget. Not an error, just "stop".
class BudgetExhausted extends Error {}

type Budget = {calls: number; max: number; deadline: number}

type PostCtx = {
  state: PlaylistState
  // Comments resolved this tick, written to Spotify in ONE batch call.
  staged: Array<{id: string; uris: string[]}>
  // Comments already handled this tick (still queued until the flush).
  skip: Set<string>
  repaired: boolean
}

let activeBudget: Budget | null = null
let nextCallAt = 0
let callsThisTick = 0

// Per-tick counters, printed as one summary line so throughput is visible.
const tickStats = {comments: 0, recs: 0, tracks: 0, noMatch: 0, added: 0}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

function commentCountKey(postId: T3): string {
  return `${COMMENT_COUNT_HASH}:${postId}`
}

function commentSeenKey(postId: T3): string {
  return `${COMMENT_SEEN_HASH}:${postId}`
}

function pendingOrderKey(postId: T3): string {
  return `${PENDING_ORDER_HASH}:${postId}`
}

// Global pacing: every outbound call (Spotify + YouTube) goes through here,
// so total request rate is bounded no matter how many posts are active.
async function pace(): Promise<void> {
  const now = Date.now()
  const wait = nextCallAt - now
  nextCallAt = Math.max(now, nextCallAt) + HTTP_PACE_MS
  if (wait > 0) await sleep(wait)
}

async function guardedFetch(
  url: string | URL,
  init?: RequestInit,
): Promise<Response> {
  const budget = activeBudget

  if (budget) {
    if (budget.calls >= budget.max || Date.now() > budget.deadline) {
      throw new BudgetExhausted('tick budget used up')
    }
    budget.calls++
  }

  await pace()

  if (budget && Date.now() > budget.deadline) {
    throw new BudgetExhausted('tick deadline reached')
  }

  callsThisTick++
  await recordCall()

  let response: Response

  try {
    response = await fetch(url, init)
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    // Devvit's platform limiter surfaces as a thrown error, not a 429.
    if (/too many requests|rate.?limit/i.test(message)) {
      throw new RateLimitError(message)
    }
    throw error
  }

  if (response.status === 429) {
    const retryAfter = Number(response.headers.get('retry-after'))
    throw new RateLimitError(
      `HTTP 429 from ${new URL(String(url)).host}`,
      Number.isFinite(retryAfter) && retryAfter > 0
        ? Math.min(retryAfter * 1000, 60 * 60 * 1000)
        : undefined,
    )
  }

  if (response.status >= 500) {
    throw new RateLimitError(
      `HTTP ${response.status} from ${new URL(String(url)).host}`,
      30 * 1000,
    )
  }

  return response
}

function hourBucket(now: number): number {
  return Math.floor(now / 3_600_000)
}

async function recordCall(): Promise<void> {
  try {
    await redis.hIncrBy(CALLS_HASH, `c:${hourBucket(Date.now())}`, 1)
  } catch {
    // counters are best-effort diagnostics, never a reason to fail a call
  }
}

async function readCallLog(
  now: number,
): Promise<{lastHour: number; last24h: number}> {
  const raw = (await redis.hGetAll(CALLS_HASH)) as Record<string, string>
  const current = hourBucket(now)
  const fraction = (now % 3_600_000) / 3_600_000
  let thisHour = 0
  let prevHour = 0
  let total = 0
  const stale: string[] = []

  for (const [field, value] of Object.entries(raw)) {
    const bucket = Number(field.slice(2))
    const count = Number(value) || 0

    if (!Number.isFinite(bucket) || bucket < current - 26) {
      stale.push(field)
      continue
    }

    if (bucket === current) thisHour = count
    else if (bucket === current - 1) prevHour = count
    if (bucket > current - 24) total += count
  }

  if (stale.length > 0) await redis.hDel(CALLS_HASH, stale)

  return {
    lastHour: Math.round(thisHour + prevHour * (1 - fraction)),
    last24h: total,
  }
}

async function getHourlyAllowance(): Promise<number> {
  const raw = Number((await redis.hGet(META_HASH, 'hourlyAllowance')) ?? 0)
  if (raw < HOURLY_ALLOWANCE_MIN) return HOURLY_ALLOWANCE_START
  return Math.min(raw, HOURLY_ALLOWANCE_MAX)
}

async function tripBreaker(
  error: RateLimitError,
  callsMade = 0,
): Promise<number> {
  const now = Date.now()
  const level = Number((await redis.hGet(META_HASH, 'backoffLevel')) ?? 0)
  const backoff =
    error.retryAfterMs ?? Math.min(60 * 1000 * 2 ** level, MAX_BACKOFF_MS)

  // Gentle decrease (-15%), once per incident: a cluster of trips within
  // TRIP_COOLDOWN_MS must not compound into a collapse.
  const usage = await readCallLog(now)
  const before = await getHourlyAllowance()
  const lastTripAt = Number((await redis.hGet(META_HASH, 'lastTripAt')) ?? 0)
  const after =
    now - lastTripAt < TRIP_COOLDOWN_MS
      ? before
      : Math.max(HOURLY_ALLOWANCE_MIN, Math.floor(before * 0.85))

  await redis.hSet(META_HASH, {
    pausedUntil: String(now + backoff),
    backoffLevel: String(level + 1),
    hourlyAllowance: String(after),
    lastTripAt: String(now),
  })

  console.log(
    `⛔ Rate limited after ${callsMade} call(s) this tick (${error.message}). Calls sent: ${usage.lastHour} in the last hour, ${usage.last24h} in the last 24h. Hourly allowance ${before} → ${after}. Pausing ${Math.round(backoff / 1000)}s`,
  )

  return backoff
}

async function getAccessTokenCached(refreshToken: string): Promise<string> {
  const token = await redis.hGet(META_HASH, 'accessToken')
  const exp = Number((await redis.hGet(META_HASH, 'accessTokenExp')) ?? 0)

  if (token && exp > Date.now() + 60 * 1000) {
    return token
  }

  const fresh = await getSpotifyAccessToken(refreshToken)

  await redis.hSet(META_HASH, {
    accessToken: fresh,
    accessTokenExp: String(Date.now() + 55 * 60 * 1000),
  })

  return fresh
}

function canonicalYoutubeUrl(url: string): string | null {
  const match = url.match(/(?:[?&]v=|youtu\.be\/)([A-Za-z0-9_-]{11})/)
  return match?.[1] ? `https://www.youtube.com/watch?v=${match[1]}` : null
}

function cacheKey(item: Recommendation): string {
  switch (item.kind) {
    case 'spotifyUrl':
      return `u:${item.url.match(/track\/([A-Za-z0-9]+)/i)?.[1] ?? item.url}`
    case 'youtubeUrl':
      return `y:${canonicalYoutubeUrl(item.url) ?? item.url}`
    case 'search':
      return `s:${normalizeText(item.song)}|${normalizeText(item.artist)}`
    case 'searchText':
      return `t:${normalizeText(item.text)}`
  }
}

// undefined = not cached, null = cached "no match", string = track URI
async function getCachedLookup(
  key: string,
): Promise<string | null | undefined> {
  const hit = await redis.hGet(CACHE_HASH, key)
  if (!hit) return undefined
  return hit === '-' ? null : hit
}

async function lookupRecommendation(
  accessToken: string,
  item: Recommendation,
): Promise<string | null> {
  if (item.kind === 'spotifyUrl') {
    // The link already contains the track id: build the URI, zero API calls.
    const id = item.url.match(/track\/([A-Za-z0-9]{22})/)?.[1]
    if (id) return `spotify:track:${id}`
    return spotifyTrackFromUrl(accessToken, item.url)
  }

  if (item.kind === 'youtubeUrl') {
    const details = await youtubeDetails(item.url)
    if (!details) return null

    const title = details.song
      .replace(
        /[([](?:official|lyric|lyrics|audio|video|music video|hd|4k|visuali[sz]er)[^)\]]*[)\]]/gi,
        '',
      )
      .replace(
        /\b(?:official\s+(?:music\s+)?(?:video|audio)|lyrics?|hd|4k)\b/gi,
        '',
      )
      .trim()

    const split = title.split(/\s+[-–—]\s+/)
    if (split.length >= 2 && split[0] && split[1]) {
      return searchSpotifyTrack(accessToken, split[0], split.slice(1).join(' '))
    }

    return searchSpotifyText(accessToken, `${title} ${details.artist}`)
  }

  if (item.kind === 'searchText') {
    return searchSpotifyText(accessToken, item.text)
  }

  return searchSpotifyTrack(accessToken, item.song, item.artist)
}

async function resolveRecommendations(
  accessToken: string,
  recommendations: Recommendation[],
): Promise<string[]> {
  const uris: string[] = []

  for (const item of recommendations) {
    const key = cacheKey(item)

    try {
      let uri = await getCachedLookup(key)

      if (uri === undefined) {
        uri = await lookupRecommendation(accessToken, item)
        // Only definitive answers are cached (a hit or a clean "no match").
        await redis.hSet(CACHE_HASH, {[key]: uri ?? '-'})
      }

      if (uri && !uris.includes(uri)) {
        uris.push(uri)
      }
    } catch (error) {
      // Rate limits and budget stops must bubble up so the comment stays
      // queued. Anything else is a bad query for this one song: skip it.
      if (error instanceof RateLimitError || error instanceof BudgetExhausted) {
        throw error
      }

      console.log(
        `⚠️ Recommendation lookup failed: ${
          error instanceof Error ? error.message : String(error)
        }`,
      )
    }
  }

  return uris
}

// Read-modify-write against FRESH state so the tick never clobbers a field
// the comment trigger changed meanwhile (e.g. the OP picking a title).
async function patchState(
  postId: T3,
  fn: (state: PlaylistState) => Partial<PlaylistState>,
): Promise<PlaylistState> {
  const current = await getState(postId)
  if (!current) throw new Error(`state missing for ${postId}`)

  const next: PlaylistState = {...current, ...fn(current)}
  await saveState(next)
  return next
}

async function finishComment(postId: T3, commentId: string): Promise<void> {
  await redis.hSet(`${PROCESSED_HASH}:${postId}`, {[commentId]: '1'})
  await removePendingComments(postId, [commentId])
  await redis.zRem(pendingOrderKey(postId), [commentId])
}

// The work set: ONLY posts that reached 100 comments and are inside 72 hours.
async function getActivePostIds(now: number): Promise<T3[]> {
  const entries = (await redis.hGetAll(QUALIFIED_HASH)) as Record<
    string,
    string
  >
  const alive: T3[] = []
  const expired: string[] = []

  for (const [id, until] of Object.entries(entries)) {
    if (Number(until) > now) alive.push(id as T3)
    else expired.push(id)
  }

  if (expired.length > 0) {
    await redis.hDel(QUALIFIED_HASH, expired)

    // The 72-hour window is over: stop processing and drop the queues.
    await Promise.all(
      expired.flatMap(id => [
        redis.del(commentCountKey(id as T3)),
        redis.del(commentSeenKey(id as T3)),
        redis.del(pendingOrderKey(id as T3)),
        redis.del(`${PENDING_HASH}:${id}`),
        redis.del(`${PROCESSED_HASH}:${id}`),
      ]),
    )
  }

  return alive
}

// A post becomes part of the work set the moment it reaches 100 comments,
// and only if it is still inside its 72 hours.
async function qualifyPost(
  postId: T3,
  reason: string,
): Promise<PlaylistState | null> {
  const state = await getState(postId)
  if (!state) return null
  if (Date.now() >= state.activeUntil) return null

  const next = state.thresholdReached
    ? state
    : await patchState(postId, () => ({thresholdReached: true}))

  await redis.hSet(QUALIFIED_HASH, {[postId]: String(state.activeUntil)})
  console.log(`🚀 ${postId}: reached ${COMMENT_THRESHOLD} comments (${reason})`)
  return next
}

async function acquireTickLock(): Promise<boolean> {
  const now = Date.now()

  // hSetNX is atomic: of N ticks that fire in the same second, exactly one
  // wins. (The old read-then-write lock let several run at once.)
  if ((await redis.hSetNX(META_HASH, 'tickLock', String(now))) > 0) {
    return true
  }

  const heldSince = Number((await redis.hGet(META_HASH, 'tickLock')) ?? 0)
  if (now - heldSince < LOCK_TTL_MS) return false

  // Lease expired: the holder crashed. Take over.
  await redis.hDel(META_HASH, ['tickLock'])
  return (await redis.hSetNX(META_HASH, 'tickLock', String(now))) > 0
}

async function releaseTickLock(): Promise<void> {
  await redis.hDel(META_HASH, ['tickLock'])
}

async function scheduleTick(
  delayMs: number,
  forceImmediate = false,
): Promise<void> {
  const now = Date.now()
  const pausedUntil = Number((await redis.hGet(META_HASH, 'pausedUntil')) ?? 0)

  const requestedAt = now + Math.max(0, delayMs)
  const runAt = Math.max(
    requestedAt,
    pausedUntil > now ? pausedUntil + 2000 : 0,
  )
  const queued = Number((await redis.hGet(META_HASH, 'nextTickAt')) ?? 0)

  // Threshold crossing can request an immediate tick. Only suppress it when
  // another tick is already due within ~2 seconds.
  if (forceImmediate) {
    if (queued > now && queued <= now + 2000) return
  } else if (queued > now && queued <= runAt + 5000) {
    return
  }

  await scheduler.runJob({
    name: 'process-playlist-post',
    data: {action: 'tick'},
    runAt: new Date(runAt),
  })

  await redis.hSet(META_HASH, {nextTickAt: String(runAt)})
}

// Exactly ONE watchdog chain can be alive: every job carries a generation
// token, and a job whose token is not the current one just stops. (Old builds
// let every comment trigger start another chain, which is how dozens of
// watchdogs piled up and fired every few seconds.)
async function scheduleWatchdog(delayMs: number): Promise<void> {
  const now = Date.now()
  const runAt = now + Math.max(0, delayMs)
  const gen = `${now}-${Math.random().toString(36).slice(2, 8)}`

  await scheduler.runJob({
    name: 'process-playlist-post',
    data: {action: 'watchdog', gen},
    runAt: new Date(runAt),
  })

  await redis.hSet(META_HASH, {watchdogGen: gen, watchdogAt: String(runAt)})
}

async function ensureWatchdog(): Promise<void> {
  const at = Number((await redis.hGet(META_HASH, 'watchdogAt')) ?? 0)

  // Alive if the next run is still ahead of us or ran within two intervals.
  if (at > Date.now() - 2 * WATCHDOG_INTERVAL_MS) return

  await scheduleWatchdog(1000)
}

// Cancels leftover jobs from older builds: stray watchdogs without a token,
// and any playlist-cleanup jobs (that feature is gone). Bounded per run.
async function purgeLegacyJobs(): Promise<number> {
  const currentGen = await redis.hGet(META_HASH, 'watchdogGen')
  const jobs = await scheduler.listJobs()
  let cancelled = 0

  for (const job of jobs) {
    if (cancelled >= 150) break
    if (job.name !== 'process-playlist-post') continue

    const data = (job.data ?? {}) as {action?: string; gen?: string}
    const stale =
      data.action === 'playlist-cleanup' ||
      (data.action === 'watchdog' && (!data.gen || data.gen !== currentGen))

    if (!stale) continue

    try {
      await scheduler.cancelJob(job.id)
      cancelled++
    } catch {
      // already ran or gone
    }
  }

  if (cancelled > 0)
    console.log(`🧹 Cancelled ${cancelled} leftover scheduled job(s)`)
  return cancelled
}

async function backfillPost(postId: T3, pc: PostCtx): Promise<void> {
  const legacy = await getLegacyProcessed(postId)
  const done = (await redis.hGetAll(`${PROCESSED_HASH}:${postId}`)) as Record<
    string,
    string
  >

  const comments = await reddit
    .getComments({postId, limit: 1000, pageSize: 100, sort: 'new'})
    .all()

  const withSongs: PendingComment[] = []

  for (const comment of comments) {
    if (extractRecommendations(comment.body).length === 0) continue

    withSongs.push({
      id: comment.id,
      body: comment.body,
      createdAt: comment.createdAt.getTime(),
    })
  }

  const have = pc.state.trackUris.length
  const lowYield = have < withSongs.length * 0.4

  const found = withSongs.filter(
    comment => lowYield || !(legacy[comment.id] || done[comment.id]),
  )

  // Keep each Redis write comfortably below the platform request-size limit.
  for (let i = 0; i < found.length; i += 100) {
    const chunk = found.slice(i, i + 100)

    await redis.hSet(
      `${PENDING_HASH}:${postId}`,
      Object.fromEntries(
        chunk.map(comment => [comment.id, JSON.stringify(comment)]),
      ),
    )

    await redis.zAdd(
      pendingOrderKey(postId),
      ...chunk.map(comment => ({
        member: comment.id,
        score: comment.createdAt,
      })),
    )
  }

  pc.state = await patchState(postId, () => ({
    backfilled: true,
    rescanned: true,
  }))

  console.log(
    `📚 Rescanned ${postId}: ${withSongs.length} comment(s) with songs, ${have} track(s) in the playlist → ${
      lowYield
        ? 'low yield, re-queued all of them'
        : 'queued the unprocessed ones'
    } (${found.length} queued)`,
  )
}

async function ensurePlaylist(
  postId: T3,
  ctx: Map<T3, PostCtx | null>,
  accessToken: string,
): Promise<void> {
  const pc = await loadPost(postId, ctx)
  if (!pc) return

  // Only posts that crossed 100 comments are ever in the work set.
  if (!pc.state.created && !pc.state.thresholdReached) return

  const state = pc.state

  // Fully set up already: nothing to do, and no HTTP or Reddit calls.
  if (state.created && state.playlistId && state.replyCommentId) return

  await createPlaylistForPost(postId, pc, accessToken, [])
}

async function recoverExistingPlaylistReply(
  postId: T3,
  playlistUrl: string,
): Promise<string | null> {
  try {
    // Crash-recovery only: a prior Reddit reply may have succeeded while the
    // state write did not. Reconcile before creating another comment.
    const comments = await reddit
      .getComments({postId, limit: 1000, pageSize: 100, sort: 'new'})
      .all()

    for (const comment of comments) {
      if (
        comment.body.includes(playlistUrl) &&
        /your playlist is ready/i.test(comment.body)
      ) {
        return comment.id
      }
    }
  } catch (error) {
    console.error(
      `⚠️ Could not reconcile existing playlist comment for ${postId}: ${
        error instanceof Error ? error.message : error
      }`,
    )
  }

  return null
}

// Idempotent: every step is saved the moment it succeeds, so a rate limit
// halfway through resumes next tick instead of making a duplicate playlist.
async function createPlaylistForPost(
  postId: T3,
  pc: PostCtx,
  accessToken: string,
  uris: string[],
): Promise<void> {
  const needsCustomTitle = isGenericPlaylistTitle(pc.state.postTitle)

  if (!pc.state.playlistId) {
    const subredditName = pc.state.subredditName ?? 'SongsThatFeelLikeThis'

    const playlist = await createSpotifyPlaylist(
      accessToken,
      needsCustomTitle
        ? makeTemporaryPlaylistTitle(postId)
        : pc.state.postTitle.slice(0, 100),
      `Playlist created from recommendations on r/${subredditName}. Original Reddit title: ${pc.state.postTitle}`,
    )

    pc.state = await patchState(postId, () => ({
      playlistId: playlist.id,
      playlistUrl: playlist.url,
    }))
  }

  const playlistId = pc.state.playlistId
  const playlistUrl = pc.state.playlistUrl

  if (!playlistId || !playlistUrl) {
    throw new Error(`playlist ids missing for ${postId}`)
  }

  const fresh = uris.filter(uri => !pc.state.trackUris.includes(uri))

  if (fresh.length > 0) {
    await addTracksToSpotifyPlaylist(accessToken, playlistId, fresh)
    pc.state = await patchState(postId, state => ({
      trackUris: [...state.trackUris, ...fresh],
    }))
  }

  if (!pc.state.replyCommentId) {
    const recoveredReplyId = await recoverExistingPlaylistReply(
      postId,
      playlistUrl,
    )

    if (recoveredReplyId) {
      pc.state = await patchState(postId, () => ({
        replyCommentId: recoveredReplyId,
        created: true,
      }))
      console.log(`♻️ Recovered existing playlist comment for ${postId}`)
    }
  }

  if (!pc.state.replyCommentId) {
    const reply = await reddit.submitComment({
      id: postId,
      text: needsCustomTitle
        ? `🎵 **Your playlist is ready!**\n\n[Listen on Spotify](${playlistUrl})\n\nIt will keep filling up with new recommendations for 72 hours after this post was created.\n\n**Want to give this playlist a name?**\nReply directly to this comment with the title you'd like.`
        : `🎵 **Your playlist is ready!**\n\n[Listen on Spotify](${playlistUrl})\n\nIt will keep filling up with new recommendations for 72 hours after this post was created.`,
      runAs: 'APP',
    })

    try {
      await reply.distinguish(true)
      console.log('📌 Playlist comment pinned')
    } catch (error) {
      console.log(
        `⚠️ Could not pin playlist comment: ${
          error instanceof Error ? error.message : error
        }`,
      )
    }

    pc.state = await patchState(postId, () => ({
      replyCommentId: reply.id,
      created: true,
      awaitingTitle: needsCustomTitle,
      fallbackApplied: false,
    }))

    if (needsCustomTitle) {
      try {
        await scheduler.runJob({
          name: 'process-playlist-post',
          data: {postId, action: 'title-fallback'},
          runAt: new Date(Date.now() + TITLE_FALLBACK_DELAY_MS),
        })
      } catch (error) {
        console.error(
          `⚠️ Could not schedule title fallback for ${postId}: ${
            error instanceof Error ? error.message : error
          }`,
        )
      }
    }
  }

  // If a crash happened after the Reddit reply succeeded but before the final
  // state write, heal the created flag on the next tick.
  if (pc.state.replyCommentId && !pc.state.created) {
    pc.state = await patchState(postId, () => ({created: true}))
  }

  console.log(`🎉 Playlist ready: ${playlistUrl}`)
}

// Handles exactly ONE queued comment for a post. Returns true if it
// consumed one (so the round-robin loop keeps going).
async function loadPost(
  postId: T3,
  ctx: Map<T3, PostCtx | null>,
): Promise<PostCtx | null> {
  let pc = ctx.get(postId)

  if (pc === undefined) {
    const state = await getState(postId)

    if (!state || Date.now() >= state.activeUntil) {
      ctx.set(postId, null)
      return null
    }

    pc = {state, staged: [], skip: new Set<string>(), repaired: false}
    ctx.set(postId, pc)
  }

  return pc
}

async function rebuildPendingOrderIndex(postId: T3): Promise<number> {
  const hashKey = `${PENDING_HASH}:${postId}`
  const orderKey = pendingOrderKey(postId)
  let cursor = 0
  let indexed = 0

  do {
    const scanned = await redis.hScan(hashKey, cursor, undefined, 100)
    const members: Array<{member: string; score: number}> = []

    for (const entry of scanned.fieldValues) {
      try {
        const item = JSON.parse(entry.value) as PendingComment
        if (item.id && Number.isFinite(item.createdAt)) {
          members.push({member: item.id, score: item.createdAt})
        }
      } catch {
        await redis.hDel(hashKey, [entry.field])
      }
    }

    if (members.length > 0) {
      await redis.zAdd(orderKey, ...members)
      indexed += members.length
    }

    cursor = scanned.cursor
  } while (cursor !== 0)

  return indexed
}

async function repairPendingIndex(postId: T3): Promise<void> {
  const hashCount = await redis.hLen(`${PENDING_HASH}:${postId}`)
  if (hashCount === 0) return

  const orderCount = await redis.zCard(pendingOrderKey(postId))
  if (hashCount <= orderCount) return

  // At most once per 30 min per post, so a permanent mismatch can never turn
  // into a full scan on every tick.
  const key = `index-rebuilt:${postId}`
  const last = Number((await redis.hGet(META_HASH, key)) ?? 0)
  if (Date.now() - last < 30 * 60 * 1000) return

  await redis.hSet(META_HASH, {[key]: String(Date.now())})
  const rebuilt = await rebuildPendingOrderIndex(postId)
  console.log(`🧩 Rebuilt the pending index for ${postId}: ${rebuilt} entries`)
}

// Oldest pending comment that is not in `skip` (already handled this tick).
async function getNextPendingComment(
  postId: T3,
  skip: Set<string>,
): Promise<PendingComment | null> {
  const orderKey = pendingOrderKey(postId)
  const hashKey = `${PENDING_HASH}:${postId}`
  const doneKey = `${PROCESSED_HASH}:${postId}`

  for (let attempt = 0; attempt < 5; attempt++) {
    const ranked = (await redis.zRange(orderKey, 0, skip.size + 9, {
      by: 'rank',
    })) as Array<{member: string; score: number}>

    if (ranked.length === 0) return null

    let cleaned = false

    for (const entry of ranked) {
      const commentId = entry.member
      if (skip.has(commentId)) continue

      const raw = await redis.hGet(hashKey, commentId)

      if (!raw) {
        await redis.zRem(orderKey, [commentId])
        cleaned = true
        continue
      }

      if (await redis.hGet(doneKey, commentId)) {
        await redis.hDel(hashKey, [commentId])
        await redis.zRem(orderKey, [commentId])
        cleaned = true
        continue
      }

      try {
        return JSON.parse(raw) as PendingComment
      } catch {
        await redis.hDel(hashKey, [commentId])
        await redis.zRem(orderKey, [commentId])
        cleaned = true
      }
    }

    if (!cleaned) return null
  }

  return null
}

// Resolves ONE queued comment and stages its tracks. The comment stays in
// Redis until the tick's batched add succeeds, so nothing is lost on a block.
async function stepPost(
  postId: T3,
  ctx: Map<T3, PostCtx | null>,
  accessToken: string,
): Promise<boolean> {
  if (activeBudget && Date.now() > activeBudget.deadline) {
    throw new BudgetExhausted('tick deadline reached')
  }

  const pc = await loadPost(postId, ctx)
  if (!pc?.state.created) return false

  if (!pc.repaired) {
    pc.repaired = true
    await repairPendingIndex(postId)
  }

  if (!pc.state.rescanned) {
    await backfillPost(postId, pc)
  }

  const next = await getNextPendingComment(postId, pc.skip)
  if (!next) return false

  const wanted = extractRecommendations(next.body)
  const uris = await resolveRecommendations(accessToken, wanted)

  pc.staged.push({id: next.id, uris})
  pc.skip.add(next.id)

  tickStats.comments++
  tickStats.recs += wanted.length
  tickStats.tracks += uris.length
  if (uris.length === 0) tickStats.noMatch++

  return true
}

// Adds tracks, splitting the batch when Spotify rejects it with a 400 so one
// bad URI can never block the rest. Any other failure (playlist gone, auth,
// rate limit) is rethrown and nothing is dropped.
async function addWithBisect(
  accessToken: string,
  playlistId: string,
  uris: string[],
): Promise<string[]> {
  if (uris.length === 0) return []

  try {
    await addTracksToSpotifyPlaylist(accessToken, playlistId, uris)
    return uris
  } catch (error) {
    if (error instanceof RateLimitError || error instanceof BudgetExhausted) {
      throw error
    }

    const message = error instanceof Error ? error.message : String(error)
    const status = Number(/\((\d{3})\)/.exec(message)?.[1] ?? 0)
    if (status !== 400) throw error

    if (uris.length === 1) {
      console.log(`⚠️ Dropping track Spotify rejected: ${uris[0]}`)
      return []
    }

    const mid = Math.ceil(uris.length / 2)
    const left = await addWithBisect(
      accessToken,
      playlistId,
      uris.slice(0, mid),
    )
    const right = await addWithBisect(accessToken, playlistId, uris.slice(mid))
    return [...left, ...right]
  }
}

async function flushStaged(
  postId: T3,
  pc: PostCtx,
  accessToken: string,
): Promise<void> {
  if (pc.staged.length === 0) return

  const known = new Set(pc.state.trackUris)
  const fresh = [...new Set(pc.staged.flatMap(item => item.uris))].filter(
    uri => !known.has(uri),
  )

  let added = 0

  if (fresh.length > 0) {
    const playlistId = pc.state.playlistId
    if (!playlistId) throw new Error(`no playlist id for ${postId}`)

    for (let i = 0; i < fresh.length; i += 100) {
      const ok = await addWithBisect(
        accessToken,
        playlistId,
        fresh.slice(i, i + 100),
      )

      added += ok.length

      // Saved after every batch, so a block half-way never repeats work.
      pc.state = await patchState(postId, state => ({
        trackUris: [
          ...state.trackUris,
          ...ok.filter(u => !state.trackUris.includes(u)),
        ],
      }))
    }
  }

  // Only now is it safe to forget these comments: their tracks are on Spotify.
  for (const item of pc.staged) {
    await finishComment(postId, item.id)
  }

  tickStats.added += added
  console.log(
    `🔄 ${postId}: ${pc.staged.length} comment(s) done, ${added} new track(s) added`,
  )

  pc.staged = []
}

async function maybeSweep(now: number): Promise<void> {
  if (SWEEP_INTERVAL_MS <= 0) return

  const last = Number((await redis.hGet(META_HASH, 'lastSweepAt')) ?? 0)
  if (now - last < SWEEP_INTERVAL_MS) return

  await redis.hSet(META_HASH, {lastSweepAt: String(now)})

  try {
    await sweepSubreddit(now)
  } catch (error) {
    console.error(
      `⚠️ Subreddit sweep failed: ${error instanceof Error ? error.message : error}`,
    )
  }
}

// Safety net behind the comment triggers. One listing (a few pages, no
// per-post calls) finds posts under 72h that reached 100 comments. Posts under
// 100 are ignored completely: no state, no queue, no work.
async function sweepSubreddit(now: number): Promise<void> {
  const subredditName =
    context.subredditName ||
    (await redis.hGet(META_HASH, 'subredditName')) ||
    'SongsThatFeelLikeThis'

  const posts = await reddit
    .getNewPosts({subredditName, limit: SWEEP_POST_LIMIT, pageSize: 100})
    .all()

  let qualified = 0

  for (const post of posts) {
    const createdAt = post.createdAt.getTime()

    if (now - createdAt >= ACTIVE_WINDOW_MS) continue
    if (post.numberOfComments < COMMENT_THRESHOLD) continue

    const postId = post.id as T3
    let state = await getState(postId)

    if (state?.created) {
      await redis.hSet(QUALIFIED_HASH, {[postId]: String(state.activeUntil)})
      continue
    }

    if (!state) {
      state = {
        postId,
        postTitle: post.title,
        postAuthorName: post.authorName,
        subredditName: post.subredditName,
        createdAt,
        activeUntil: createdAt + ACTIVE_WINDOW_MS,
        trackUris: [],
        created: false,
        awaitingTitle: false,
        fallbackApplied: false,
      }

      await redis.hSetNX(STATE_HASH, postId, JSON.stringify(state))
    }

    if (!state.thresholdReached) {
      await qualifyPost(postId, `${post.numberOfComments} comments per Reddit`)
      qualified++
    } else {
      await redis.hSet(QUALIFIED_HASH, {[postId]: String(state.activeUntil)})
    }
  }

  if (qualified > 0) {
    console.log(`🛰️ Sweep: ${qualified} post(s) newly qualified for a playlist`)
    await scheduleTick(0, true)
  }
}

// One-time clean-up when this build first runs. Safe to repeat: every step is
// idempotent and only the first caller does the work.
async function migrateOnce(): Promise<void> {
  if (await redis.hGet(META_HASH, 'v11Done')) return
  if ((await redis.hSetNX(META_HASH, 'v11Started', String(Date.now()))) === 0) {
    return
  }

  try {
    const legacy = (await redis.hGetAll(LEGACY_ACTIVE_HASH)) as Record<
      string,
      string
    >
    const now = Date.now()
    let kept = 0
    let cleared = 0

    for (const [id] of Object.entries(legacy)) {
      const state = await getState(id as T3)

      const qualifies =
        !!state &&
        now < state.activeUntil &&
        (state.created || state.thresholdReached === true)

      if (qualifies && state) {
        await redis.hSet(QUALIFIED_HASH, {[id]: String(state.activeUntil)})
        kept++
        continue
      }

      // Under 100 comments (or past 72h): not part of the work set any more.
      await redis.del(`${PENDING_HASH}:${id}`)
      await redis.del(pendingOrderKey(id as T3))
      await redis.del(`${PROCESSED_HASH}:${id}`)
      cleared++
    }

    await redis.del(LEGACY_ACTIVE_HASH)
    await redis.del(LEGACY_CLEANUP_HASH)
    await redis.hSet(META_HASH, {
      pausedUntil: '0',
      backoffLevel: '0',
      tickLock: '',
    })
    await redis.hDel(META_HASH, ['tickLock'])

    console.log(
      `🧽 Migration: ${kept} qualified post(s) kept, ${cleared} under-100 post(s) removed from the work set, playlist cleanup data deleted`,
    )

    try {
      await purgeLegacyJobs()
    } catch (error) {
      console.error(
        `⚠️ Could not purge old jobs: ${error instanceof Error ? error.message : error}`,
      )
    }

    await scheduleWatchdog(1000)
    await scheduleTick(2000, true)
    await redis.hSet(META_HASH, {v11Done: '1'})
  } catch (error) {
    // Let the next request retry the migration.
    await redis.hDel(META_HASH, ['v11Started'])
    throw error
  }
}

async function runTick(): Promise<number | null> {
  const now = Date.now()

  const pausedUntil = Number((await redis.hGet(META_HASH, 'pausedUntil')) ?? 0)
  if (pausedUntil > now) return Math.max(pausedUntil - now, 5 * 1000)

  await maybeSweep(now)

  const postIds = await getActivePostIds(now)
  if (postIds.length === 0) return null

  const refreshToken = await settings.get<string>('spotify-refresh-token')

  if (!refreshToken) {
    console.log('⚠️ Spotify refresh token is not configured')
    return IDLE_TICK_INTERVAL_MS
  }

  // Rotate the starting post every tick so no post is always first.
  const cursor = Number((await redis.hGet(META_HASH, 'rrCursor')) ?? 0)
  await redis.hSet(META_HASH, {rrCursor: String(cursor + 1)})
  const offset = cursor % postIds.length
  const ordered = [...postIds.slice(offset), ...postIds.slice(0, offset)]

  tickStats.comments = 0
  tickStats.recs = 0
  tickStats.tracks = 0
  tickStats.noMatch = 0
  tickStats.added = 0
  callsThisTick = 0

  const usage = await readCallLog(now)
  const allowance = await getHourlyAllowance()
  const remaining = Math.max(0, allowance - usage.lastHour)

  // Playlist creation has its own small budget that is never eaten by song
  // lookups, so a viral post can't starve it.
  const creationBudget: Budget = {
    calls: 0,
    max: CREATION_CALLS_PER_TICK,
    deadline: now + TICK_BUDGET_MS,
  }
  // Spread the allowance evenly: no more than this tick's fair share, so the
  // bot trickles all hour instead of bursting early and idling until rollover.
  const evenShare = Math.max(
    1,
    Math.ceil((allowance / 3600) * TICK_CYCLE_SECONDS),
  )
  const workBudget: Budget = {
    calls: 0,
    max: Math.min(MAX_HTTP_CALLS_PER_TICK, evenShare, remaining),
    deadline: now + TICK_BUDGET_MS,
  }

  const ctx = new Map<T3, PostCtx | null>()
  let tripped: RateLimitError | null = null

  try {
    activeBudget = creationBudget
    const accessToken = await getAccessTokenCached(refreshToken)

    for (const postId of ordered) {
      try {
        await ensurePlaylist(postId, ctx, accessToken)
      } catch (error) {
        if (
          error instanceof RateLimitError ||
          error instanceof BudgetExhausted
        ) {
          throw error
        }

        console.error(
          `⚠️ ${postId} playlist creation failed: ${
            error instanceof Error ? error.message : String(error)
          }`,
        )
        ctx.set(postId, null)
      }
    }

    if (workBudget.max > 0) {
      activeBudget = workBudget

      // Round-robin: one comment per post per pass, repeated until this
      // tick's call budget is used. Posts advance side by side.
      work: while (true) {
        let progressed = false

        for (const postId of ordered) {
          try {
            if (await stepPost(postId, ctx, accessToken)) progressed = true
          } catch (error) {
            if (error instanceof RateLimitError) {
              tripped = error
              break work
            }

            if (error instanceof BudgetExhausted) break work

            console.error(
              `⚠️ ${postId} step failed: ${
                error instanceof Error ? error.message : String(error)
              }`,
            )
            ctx.set(postId, null)
          }
        }

        if (!progressed) break
      }
    }
  } catch (error) {
    if (error instanceof RateLimitError) {
      tripped = error
    } else if (!(error instanceof BudgetExhausted)) {
      console.error(
        `tick loop failed; ${error instanceof Error ? error.stack : error}`,
      )
    }
  } finally {
    activeBudget = null
  }

  if (tripped) return tripBreaker(tripped, callsThisTick)

  // One batched add per post. Staged comments only leave Redis after this.
  try {
    const accessToken = await getAccessTokenCached(refreshToken)

    for (const [postId, pc] of ctx) {
      if (!pc) continue

      try {
        await flushStaged(postId, pc, accessToken)
      } catch (error) {
        if (error instanceof RateLimitError) throw error

        console.error(
          `⚠️ ${postId} could not add tracks (comments stay queued): ${
            error instanceof Error ? error.message : String(error)
          }`,
        )
      }
    }
  } catch (error) {
    if (error instanceof RateLimitError)
      return tripBreaker(error, callsThisTick)

    console.error(
      `flush failed; ${error instanceof Error ? error.stack : error}`,
    )
  }

  if (callsThisTick > 0) {
    await redis.hSet(META_HASH, {backoffLevel: '0'})
  }

  // Creep the allowance back up only when it is actually being used and
  // there has been no block for a while.
  const lastTripAt = Number((await redis.hGet(META_HASH, 'lastTripAt')) ?? 0)
  const lastGrowAt = Number((await redis.hGet(META_HASH, 'lastGrowAt')) ?? 0)

  if (
    now - lastTripAt > ALLOWANCE_GROW_AFTER_MS &&
    now - lastGrowAt > ALLOWANCE_GROW_AFTER_MS &&
    usage.lastHour + callsThisTick >= allowance * 0.7 &&
    allowance < HOURLY_ALLOWANCE_MAX
  ) {
    const grown = Math.min(HOURLY_ALLOWANCE_MAX, Math.ceil(allowance * 1.1))
    await redis.hSet(META_HASH, {
      hourlyAllowance: String(grown),
      lastGrowAt: String(now),
    })
    console.log(`📈 Hourly allowance ${allowance} → ${grown}`)
  }

  let queued = 0

  for (const pc of ctx.values()) {
    if (!pc?.state.created) continue
    queued += await redis.hLen(`${PENDING_HASH}:${pc.state.postId}`)
  }

  console.log(
    `📊 tick: ${tickStats.comments} comment(s) → ${tickStats.tracks} track(s) found, ${tickStats.noMatch} with no confident match, ${tickStats.added} added; ${callsThisTick} call(s) this tick, ${usage.lastHour + callsThisTick}/${allowance} this hour; ${queued} comment(s) still queued`,
  )

  if (remaining <= 0 && queued > 0) {
    console.log('⏳ Hourly allowance reached; resuming as the window rolls')
    return 60 * 1000
  }

  return queued > 0 ? TICK_INTERVAL_MS : IDLE_TICK_INTERVAL_MS
}

async function runWatchdog(): Promise<number> {
  const now = Date.now()

  await maybeSweep(now)

  const active = await getActivePostIds(now)

  if (active.length > 0) {
    // Recover the worker if the previous scheduler chain died silently.
    await scheduleTick(0, true)
  }

  let purged = 0

  try {
    purged = await purgeLegacyJobs()
  } catch (error) {
    console.error(
      `⚠️ Could not list scheduled jobs: ${error instanceof Error ? error.message : error}`,
    )
  }

  console.log(
    `🩺 Watchdog: ${active.length} qualified post(s) in the work set, ${purged} stale job(s) removed`,
  )

  return WATCHDOG_INTERVAL_MS
}

async function applyTitleFallback(postId: T3): Promise<void> {
  const state = await getState(postId)

  if (
    !state?.created ||
    !state.playlistId ||
    !state.awaitingTitle ||
    state.fallbackApplied
  ) {
    return
  }

  const refreshToken = await settings.get<string>('spotify-refresh-token')
  if (!refreshToken) return

  const accessToken = await getAccessTokenCached(refreshToken)
  const fallbackTitle = makeFallbackPlaylistTitle(state.createdAt, postId)

  await renameSpotifyPlaylist(accessToken, state.playlistId, fallbackTitle)
  await patchState(postId, () => ({fallbackApplied: true}))

  console.log(`📝 Applied fallback playlist title: ${fallbackTitle}`)
}

async function getState(postId: T3): Promise<PlaylistState | null> {
  const raw = await redis.hGet(STATE_HASH, postId)

  if (!raw) {
    return null
  }

  try {
    return JSON.parse(raw) as PlaylistState
  } catch {
    return null
  }
}

async function saveState(state: PlaylistState): Promise<void> {
  await redis.hSet(STATE_HASH, {
    [state.postId]: JSON.stringify(state),
  })
}

async function getLegacyProcessed(
  postId: T3,
): Promise<Record<string, boolean>> {
  const raw = await redis.hGet(PROCESSED_HASH, postId)

  if (!raw) {
    return {}
  }

  try {
    return JSON.parse(raw) as Record<string, boolean>
  } catch {
    return {}
  }
}

async function queueComment(
  postId: T3,
  comment: PendingComment,
): Promise<void> {
  await redis.hSet(`${PENDING_HASH}:${postId}`, {
    [comment.id]: JSON.stringify(comment),
  })

  await redis.zAdd(pendingOrderKey(postId), {
    member: comment.id,
    score: comment.createdAt,
  })
}

async function removePendingComments(
  postId: T3,
  commentIds: string[],
): Promise<void> {
  if (commentIds.length === 0) return

  await redis.hDel(`${PENDING_HASH}:${postId}`, commentIds)
}

export async function onReq(
  reqMsg: IncomingMessage,
  rspMsg: ServerResponse,
): Promise<void> {
  try {
    await route(reqMsg, rspMsg)
  } catch (err) {
    const msg = `server error; ${err instanceof Error ? err.stack : err}`

    console.error(msg)

    writeJson<ErrorRsp>(
      500,
      {
        error: msg,
        status: 500,
      },
      rspMsg,
    )
  }
}

async function route(
  reqMsg: IncomingMessage,
  rspMsg: ServerResponse,
): Promise<void> {
  const endpoint = reqMsg.url?.slice(1) as Endpoint

  const method = EndpointMethod[endpoint]

  let rsp: AnyRsp

  if (method !== reqMsg.method) {
    rsp = {
      error: 'not found',
      status: 404,
    }
  } else {
    switch (endpoint) {
      case Endpoint.GetCounter:
        rsp = await routeGetCounter()
        break

      case Endpoint.IncCounter:
        rsp = await routeInc(reqMsg)
        break

      case Endpoint.OnAppInstall:
        rsp = await routeAppInstall()
        break

      case Endpoint.OnPostSubmit:
        rsp = await routePostOrComment(reqMsg)
        break

      case Endpoint.ProcessPlaylistPost:
        rsp = await routeProcessPlaylistPost(reqMsg)
        break

      default:
        endpoint satisfies never

        rsp = {
          error: 'not found',
          status: 404,
        }

        break
    }
  }

  writeJson<PartialJsonValue>('status' in rsp ? rsp.status : 200, rsp, rspMsg)
}

async function routeGetCounter(): Promise<GetCounterRsp> {
  const t3 = context.postId

  if (!t3) {
    throw Error('no t3')
  }

  return {
    count: await dbGetCounter(t3),
  }
}

async function routeInc(reqMsg: IncomingMessage): Promise<IncCounterRsp> {
  const t3 = context.postId

  if (!t3) {
    throw Error('no t3')
  }

  const req = await readJson<IncCounterReq>(reqMsg)

  return {
    count: await dbIncCounter(t3, req.amount),
  }
}

// Comment triggers must not each queue their own tick: under load that
// produced several ticks per second. A tick reschedules itself, so only queue
// one when none is pending and none is running (the watchdog covers the rest).
async function ensureTickQueued(): Promise<void> {
  const now = Date.now()
  const queuedAt = Number((await redis.hGet(META_HASH, 'nextTickAt')) ?? 0)
  if (queuedAt > now) return

  const heldSince = Number((await redis.hGet(META_HASH, 'tickLock')) ?? 0)
  if (heldSince > 0 && now - heldSince < LOCK_TTL_MS) return

  await scheduleTick(TICK_INTERVAL_MS)
}

async function routeAppInstall(): Promise<TriggerResponse> {
  try {
    await scheduleWatchdog(1000)
    await scheduleTick(1000, true)
  } catch (error) {
    console.error(
      `⚠️ App bootstrap scheduling failed: ${
        error instanceof Error ? error.message : error
      }`,
    )
  }

  return {}
}

async function kickThresholdPlaylist(postId: T3): Promise<void> {
  const pausedUntil = Number((await redis.hGet(META_HASH, 'pausedUntil')) ?? 0)

  if (pausedUntil > Date.now()) {
    // Blocked right now: do not send a call that will just be rejected. The
    // tick is scheduled for when the pause ends and creates the playlist first.
    await scheduleTick(0, true)
    return
  }

  if (!(await acquireTickLock())) {
    // Another tick owns the global worker lease. Do not overlap Spotify work;
    // request an immediate retry so the 100th-comment playlist still gets
    // creation priority as soon as the current worker releases the lease.
    await scheduleTick(0, true)
    return
  }

  const budget: Budget = {
    calls: 0,
    max: 3,
    deadline: Date.now() + 15 * 1000,
  }
  const ctx = new Map<T3, PostCtx | null>()

  try {
    const refreshToken = await settings.get<string>('spotify-refresh-token')
    if (!refreshToken) {
      console.log(
        `⚠️ Immediate playlist creation skipped for ${postId}: Spotify token missing`,
      )
      return
    }

    activeBudget = budget
    const accessToken = await getAccessTokenCached(refreshToken)
    await ensurePlaylist(postId, ctx, accessToken)
  } catch (error) {
    if (error instanceof RateLimitError) {
      await tripBreaker(error, budget.calls)
    } else {
      console.error(
        `⚠️ Immediate playlist creation failed for ${postId}: ${
          error instanceof Error ? error.stack : error
        }`,
      )
    }
  } finally {
    activeBudget = null
    await releaseTickLock()
  }
}

async function routePostOrComment(
  reqMsg: IncomingMessage,
): Promise<TriggerResponse> {
  await migrateOnce()

  const event = await readJson<TriggerEvent>(reqMsg)

  if (event.type === 'PostSubmit') {
    const postId = event.post?.id as T3 | undefined

    if (!postId) {
      return {}
    }

    const post = await reddit.getPostById(postId)

    const state: PlaylistState = {
      postId,
      postTitle: post.title,
      postAuthorName: post.authorName,
      subredditName: post.subredditName,
      createdAt: post.createdAt.getTime(),
      activeUntil: post.createdAt.getTime() + ACTIVE_WINDOW_MS,
      trackUris: [],
      created: false,
      awaitingTitle: false,
      fallbackApplied: false,
    }

    const firstPostDelivery =
      (await redis.hSetNX(STATE_HASH, postId, JSON.stringify(state))) > 0

    if (firstPostDelivery) {
      await redis.set(commentCountKey(postId), '0')
      // Counters clean themselves up shortly after the 72-hour window.
      await redis.expire(commentCountKey(postId), 80 * 3600)
    }

    await redis.hSet(META_HASH, {subredditName: post.subredditName})

    // A new post is NOT work yet: it only counts comments (Redis, no HTTP)
    // until it reaches 100.
    console.log(`🎵 Watching post: ${postId} — ${post.title}`)

    await ensureWatchdog()

    return {}
  }

  if (event.type === 'CommentCreate') {
    const postId = event.comment?.postId as T3 | undefined
    const commentId = event.comment?.id

    if (!postId || !commentId) {
      return {}
    }

    // Redis only: no Reddit or Spotify call on this path.
    const state = await getState(postId)

    if (!state) {
      return {}
    }

    // 72-hour rule: nothing new is processed after the window.
    if (Date.now() >= state.activeUntil) {
      return {}
    }

    // OP naming reply is handled immediately, never queued.
    if (
      state.created &&
      state.playlistId &&
      state.replyCommentId &&
      state.awaitingTitle
    ) {
      const isDirectReply = event.comment?.parentId === state.replyCommentId
      const isOriginalPoster = event.comment?.author === state.postAuthorName

      if (isDirectReply && isOriginalPoster) {
        const customTitle = cleanCustomPlaylistTitle(event.comment?.body ?? '')

        if (customTitle) {
          const refreshToken = await settings.get<string>(
            'spotify-refresh-token',
          )

          if (refreshToken) {
            try {
              const accessToken = await getAccessTokenCached(refreshToken)

              await renameSpotifyPlaylist(
                accessToken,
                state.playlistId,
                customTitle,
              )

              await patchState(postId, () => ({awaitingTitle: false}))

              await redis.hSet(`${PROCESSED_HASH}:${postId}`, {
                [commentId]: '1',
              })

              console.log(`🎨 OP selected playlist title: ${customTitle}`)
            } catch (error) {
              // Never fail the trigger: the 6h title fallback still applies.
              if (error instanceof RateLimitError) await tripBreaker(error)
              console.error(
                `⚠️ OP title rename failed: ${
                  error instanceof Error ? error.message : String(error)
                }`,
              )
            }

            return {}
          }
        }
      }
    }

    let qualified = state.created || state.thresholdReached === true
    let justQualified = false

    if (!qualified) {
      // Below 100: count and stop. Nothing is queued, scheduled or looked up.
      const firstDelivery =
        (await redis.hSetNX(commentSeenKey(postId), commentId, '1')) > 0

      if (!firstDelivery) return {}

      const count = await redis.incrBy(commentCountKey(postId), 1)

      if (count === 1) {
        await redis.expire(commentSeenKey(postId), 80 * 3600)
        await redis.expire(commentCountKey(postId), 80 * 3600)
      }

      if (count < COMMENT_THRESHOLD) return {}

      const next = await qualifyPost(postId, `${count} comments seen`)
      if (!next) return {}

      qualified = true
      justQualified = true

      // Create the playlist inside this request when possible.
      await kickThresholdPlaylist(postId)
    }

    // From here the post is qualified (100+ comments, inside 72 hours).
    const alreadyProcessed = await redis.hGet(
      `${PROCESSED_HASH}:${postId}`,
      commentId,
    )

    const body = event.comment?.body ?? ''

    if (!alreadyProcessed && extractRecommendations(body).length > 0) {
      await queueComment(postId, {
        id: commentId,
        body,
        createdAt: Date.now(),
      })
    }

    await ensureWatchdog()

    if (justQualified) await scheduleTick(0, true)
    else await ensureTickQueued()
  }

  return {}
}

async function routeProcessPlaylistPost(
  reqMsg: IncomingMessage,
): Promise<TriggerResponse> {
  const event = await readJson<SchedulerEvent>(reqMsg)
  await migrateOnce()

  const action = event.data?.action ?? 'tick'
  console.log(`🧭 build tick-v11: job '${action}' received`)

  if (action === 'watchdog') {
    // Stale duplicate from an older build or chain: stop here, do not reschedule.
    const currentGen = await redis.hGet(META_HASH, 'watchdogGen')

    if (!event.data?.gen || event.data.gen !== currentGen) {
      return {}
    }

    try {
      const next = await runWatchdog()
      await scheduleWatchdog(next)
    } catch (error) {
      console.error(
        `⚠️ Watchdog failed; retrying in 60s: ${
          error instanceof Error ? error.stack : error
        }`,
      )
      await scheduleWatchdog(60 * 1000)
    }

    return {}
  }

  if (action === 'title-fallback') {
    const postId = event.data?.postId as T3 | undefined
    if (!postId) return {}

    if (!(await acquireTickLock())) {
      await scheduler.runJob({
        name: 'process-playlist-post',
        data: {action: 'title-fallback', postId},
        runAt: new Date(Date.now() + 60 * 1000),
      })
      return {}
    }

    const maintenanceBudget: Budget = {
      calls: 0,
      max: 2,
      deadline: Date.now() + 15 * 1000,
    }

    try {
      activeBudget = maintenanceBudget
      await applyTitleFallback(postId)
    } catch (error) {
      if (error instanceof RateLimitError) {
        const retry = await tripBreaker(error, maintenanceBudget.calls)
        await scheduler.runJob({
          name: 'process-playlist-post',
          data: {action: 'title-fallback', postId},
          runAt: new Date(Date.now() + retry),
        })
      } else {
        console.error(
          `⚠️ Title fallback failed for ${postId}; retrying in 1h: ${
            error instanceof Error ? error.stack : error
          }`,
        )
        await scheduler.runJob({
          name: 'process-playlist-post',
          data: {action: 'title-fallback', postId},
          runAt: new Date(Date.now() + 60 * 60 * 1000),
        })
      }
    } finally {
      activeBudget = null
      await releaseTickLock()
    }

    return {}
  }

  if (action === 'playlist-cleanup') {
    // Playlist deletion was removed. Old jobs that were scheduled by earlier
    // builds are ignored on purpose; nothing is ever deleted.
    return {}
  }

  // 'tick', and any legacy 'process' jobs queued by previous versions.
  if (!(await acquireTickLock())) {
    // Another worker is already handling the queue; do not create a second
    // concurrent Spotify worker.
    return {}
  }

  await ensureWatchdog()

  // The currently-running tick is no longer the queued tick. This prevents a
  // stale Redis timestamp from blocking recovery scheduling.
  await redis.hSet(META_HASH, {nextTickAt: '0'})

  let nextDelay: number | null = null

  try {
    nextDelay = await runTick()
  } catch (error) {
    console.error(
      `tick failed; ${error instanceof Error ? error.stack : error}`,
    )
    nextDelay = 60 * 1000
  } finally {
    await releaseTickLock()
  }

  if (nextDelay !== null) {
    try {
      await scheduleTick(nextDelay)
    } catch (error) {
      console.error(
        `could not schedule next tick; ${
          error instanceof Error ? error.message : error
        }`,
      )
    }
  }

  return {}
}

async function readJson<T>(reqMsg: IncomingMessage): Promise<T> {
  const chunks: Uint8Array[] = []

  reqMsg.on('data', chunk => chunks.push(chunk))

  await once(reqMsg, 'end')

  return JSON.parse(`${Buffer.concat(chunks)}`)
}

function writeJson<T extends PartialJsonValue>(
  status: number,
  json: Readonly<T>,
  rsp: ServerResponse,
): void {
  const body = JSON.stringify(json)

  rsp.writeHead(status, {
    'Content-Length': Buffer.byteLength(body),

    'Content-Type': 'application/json',
  })

  rsp.end(body)
}

async function createSpotifyPlaylist(
  accessToken: string,
  name: string,
  description: string,
): Promise<{
  id: string
  url: string
}> {
  const response = await spotifyFetch(
    'https://api.spotify.com/v1/me/playlists',
    {
      method: 'POST',

      headers: {
        Authorization: `Bearer ${accessToken}`,

        'Content-Type': 'application/json',
      },

      body: JSON.stringify({
        name,
        description,
        public: true,
      }),
    },
  )

  if (!response.ok) {
    throw new Error(
      `Spotify playlist creation failed (${response.status}): ${await response.text()}`,
    )
  }

  const data = (await response.json()) as {
    id: string

    external_urls?: {
      spotify?: string
    }
  }

  const url = data.external_urls?.spotify

  if (!url) {
    throw new Error('Spotify playlist URL was not returned')
  }

  console.log(`🎵 Spotify playlist created: ${url}`)

  return {
    id: data.id,
    url,
  }
}

async function addTracksToSpotifyPlaylist(
  accessToken: string,
  playlistId: string,
  trackUris: string[],
): Promise<void> {
  const uniqueUris = [...new Set(trackUris)].filter(Boolean)

  if (uniqueUris.length === 0) return
  if (uniqueUris.length > 100) {
    throw new Error(
      `addTracksToSpotifyPlaylist received ${uniqueUris.length} URIs; callers must batch at <=100`,
    )
  }

  const response = await spotifyFetch(
    `https://api.spotify.com/v1/playlists/${playlistId}/items`,
    {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        uris: uniqueUris,
      }),
    },
  )

  if (!response.ok) {
    throw new Error(
      `Spotify track adding failed (${response.status}): ${await response.text()}`,
    )
  }

  console.log(`🎵 Added ${uniqueUris.length} track(s) to ${playlistId}`)
}
