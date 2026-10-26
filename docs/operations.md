# Operations

## Required configuration

The app expects the global Devvit setting `spotify-refresh-token`, declared as a secret in `devvit.json`.

```bash
npx devvit settings set spotify-refresh-token
```

The Spotify account that authorized the refresh token must grant `playlist-modify-public`. Keep the token out of GitHub issues, screenshots, source files, and logs. If a token is exposed, revoke/rotate it in Spotify and update the Devvit secret.

## Read production logs

```bash
npx devvit logs r/SongsThatFeelLikeThis --since=1h --verbose
```

Useful log messages include:

- `reached 100 comments` — the post entered the work set.
- `Playlist ready` — the Spotify playlist and Reddit reply were created.
- `Added ... track(s)` — one or more resolved tracks were added.
- `Rate limited` — the global breaker paused requests.
- `Rescanned` or `Sweep` — the safety sweep recovered a post or comment set.

## No playlist appears

1. Confirm the post reached 100 comments and is still inside its 72-hour window.
2. Check that `spotify-refresh-token` is configured.
3. Check logs for `Rate limited`, `playlist creation failed`, or a scheduler/lease issue.
4. If the global breaker is active, let the scheduled retry run; avoid manually replaying requests in a tight loop.

## Playlist exists but songs are not arriving

1. Confirm new comments contain a recognizable song/artist string or supported music link.
2. Check logs for `no confident match`, `could not add tracks`, and `Rate limited`.
3. A newly created playlist can be empty while comments are still being resolved.
4. Duplicate track URIs are intentionally skipped.
5. Processing ends at the post’s 72-hour expiry.

## Spotify 429 responses

Spotify documents a rolling 30-second rate limit that varies by quota mode and a separate development-mode quota system. When a request returns HTTP 429, check the response’s `Retry-After` value and the Spotify Developer Dashboard. The bot’s own hourly allowance is only an internal safety budget.

- [Spotify rate limits](https://developer.spotify.com/documentation/web-api/concepts/rate-limits)
- [Spotify quota modes](https://developer.spotify.com/documentation/web-api/concepts/quota-modes)

## Testing and releases

Use `npm run playtest` with the configured development subreddit. Build with `npm run build`. `npx devvit upload` creates a private build; `npx devvit publish` submits a release through Reddit’s review flow. Neither action is triggered by a GitHub push.
