# TEMPO pipeline deep-dive slides

A [Slidev](https://sli.dev) deck walking through the pipeline for engineers
who may run one like it. It reuses the figures in `docs/reference/`.

```sh
npm install
npm run dev        # http://localhost:3030
npm run build      # static site in dist/
npm run export     # slides.pdf, needs `npx playwright install chromium` once
```

## Before presenting

- Screenshots live in `public/screenshots/`. Its README lists the files.
- `npm run shots` regenerates the titiler pair from the staging viewer,
  with and without the `where` filter at the same extent. It needs
  `npx playwright install chromium` once. Edit `shoot.mjs` to change the
  scan time, the conditions, or the extent.

## Deploy

`dist/` is a static site. For GitHub Pages under a project path, build
with `npx slidev build --base /<repo>/slides/`.
