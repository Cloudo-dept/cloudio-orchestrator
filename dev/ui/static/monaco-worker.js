// Web-worker entry point for Monaco's language services (see MonacoEnvironment in app.js).
// The worker host resolves `vs/language/json/jsonWorker.js` relative to baseUrl, so it has to be
// told where the vendored tree lives before it is imported.
self.MonacoEnvironment = { baseUrl: "/vendor/monaco/" };
importScripts("/vendor/monaco/vs/base/worker/workerMain.js");
