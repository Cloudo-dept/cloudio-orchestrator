# Vendored Monaco Editor

Powers the JSON param editors in the dev console. Vendored (not loaded from a CDN) so the console
works air-gapped — the same reason the rest of `dev/ui` has no dependencies and no build step.

- **Source:** [`monaco-editor@0.52.2`](https://www.npmjs.com/package/monaco-editor), the `min/vs`
  AMD build, copied verbatim.
- **License:** MIT (see `LICENSE`), © Microsoft Corporation.

## What was kept

Only what a JSON editor needs — 4.5 MB of the 14 MB `min/vs` tree:

| Path | Why |
| --- | --- |
| `vs/loader.js` | the AMD loader `index.html` bootstraps with |
| `vs/editor/editor.main.{js,css}` | the editor core (English strings are inlined) |
| `vs/language/json/{jsonMode,jsonWorker}.js` | the JSON language service — validation, formatting |
| `vs/base/worker/workerMain.js` | worker host, loaded by `../../monaco-worker.js` |
| `vs/base/browser/ui/codicons/…/codicon.ttf` | the editor's icon font |

Dropped: `vs/basic-languages/**` (no other language is ever requested), `vs/language/{typescript,css,html}/**`,
and the `nls.messages.*.js` translations.

## Refreshing it

```sh
curl -sL https://registry.npmjs.org/monaco-editor/-/monaco-editor-<version>.tgz | tar xz package/min package/LICENSE
rm -rf vs && mkdir -p vs/language
cp -r package/min/vs/{base,editor} vs/
cp -r package/min/vs/language/json vs/language/
cp package/min/vs/loader.js vs/
cp package/LICENSE .
```

If a future editor needs another language, copy its `vs/language/<name>` directory too and update
the table above.
