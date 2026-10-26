# Contributing

## Development setup

1. Install Node.js 24 or newer and npm.
2. Run `npm ci`.
3. Authenticate with `npx devvit login`.
4. Use `npm run playtest` with the development subreddit configured in `devvit.json`.

## Before opening a change

Run the project's static checks and build:

```bash
npm run test:types
npm run lint
npm run build
```

Unit tests are available with `npm run test:unit`.

## Security

Never commit `spotify-token.json`, refresh/access tokens, Spotify client secrets, Reddit credentials, or `.env` files. Keep the Devvit refresh token in the secret setting. Do not paste secrets into issues or pull requests.
