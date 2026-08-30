"use strict";

// The orchestrator API, proxied through server.py (same origin → no CORS).
const API = "/api/v1";
const REFRESH_MS = 5000;
const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => [...document.querySelectorAll(sel)];

// Full server-side lists, refreshed on a timer — no client-side tracking.
let workflowList = []; // [WorkflowResponse]
let runList = []; // [WorkflowRunResponse]

// --- tiny API helper ---------------------------------------------------------
async function api(method, path, body) {
  const opts = { method, headers: {} };
  if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const resp = await fetch(API + path, opts);
  const text = await resp.text();
  const data = text ? JSON.parse(text) : null;
  if (!resp.ok) {
    const detail = data && data.detail ? JSON.stringify(data.detail) : resp.statusText;
    const err = new Error(`${resp.status} — ${detail}`);
    err.status = resp.status;
    throw err;
  }
  return data;
}

// --- toasts ------------------------------------------------------------------
function toast(title, msg, kind = "ok") {
  const el = document.createElement("div");
  el.className = `toast toast-${kind}`;
  el.innerHTML = `<div class="bar"></div><div class="body">
      <div class="title"></div><div class="msg"></div></div>`;
  el.querySelector(".title").textContent = title;
  el.querySelector(".msg").textContent = msg;
  $("#toasts").appendChild(el);
  setTimeout(() => {
    el.classList.add("leaving");
    setTimeout(() => el.remove(), 250);
  }, kind === "err" ? 6000 : 3500);
}
const ok = (title, msg) => toast(title, msg, "ok");
const fail = (title, err) => toast(title, String(err.message || err), "err");

// --- drawer (details) --------------------------------------------------------
function openDrawer(title, data) {
  $("#drawer-title").textContent = title;
  $("#drawer-body").textContent =
    typeof data === "string" ? data : JSON.stringify(data, null, 2);
  $("#drawer").hidden = false;
}
function closeDrawer() { $("#drawer").hidden = true; }

// --- helpers -----------------------------------------------------------------
function pill(status) { return `<span class="pill pill-${status}">${status}</span>`; }
function esc(s) {
  return String(s == null ? "" : s).replace(/[&<>"]/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}
function csvList(raw) {
  return (raw || "").split(",").map((s) => s.trim()).filter(Boolean);
}

// --- JSON param editors (Monaco) ---------------------------------------------
// Every free-form param set the API types as `dict[str, Any]` gets an editor, so payloads are
// authored as the JSON they are sent as — nested objects and arrays included.
const jsonEditors = {}; // name -> monaco editor, populated once Monaco finishes loading

// Monaco's language services run in a worker; see static/monaco-worker.js.
self.MonacoEnvironment = { getWorkerUrl: () => "/monaco-worker.js" };

require.config({ paths: { vs: "/vendor/monaco/vs" } });
require(["vs/editor/editor.main"], () => {
  // The console is dark; repaint the editor's chrome in the same palette as styles.css.
  monaco.editor.defineTheme("orchestrator", {
    base: "vs-dark",
    inherit: true,
    rules: [],
    colors: { "editor.background": "#0d1117", "editorGutter.background": "#0d1117" },
  });
  // Every model here is a param set, so one schema covers them all: the API rejects anything
  // that is not a JSON object, and this surfaces that in the editor instead of on submit.
  monaco.languages.json.jsonDefaults.setDiagnosticsOptions({
    validate: true,
    schemas: [{
      uri: "https://orchestrator.dev/schemas/params.json",
      fileMatch: ["*"],
      schema: { type: "object" },
    }],
  });
  $$(".json-editor").forEach((el) => {
    jsonEditors[el.dataset.json] = monaco.editor.create(el, {
      value: "{}",
      language: "json",
      theme: "orchestrator",
      automaticLayout: true, // the trigger form starts hidden — relayout when it is revealed
      minimap: { enabled: false },
      scrollBeyondLastLine: false,
      lineNumbers: "off",
      folding: false,
      tabSize: 2,
      fontSize: 12.5,
      fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace",
      renderLineHighlight: "none",
      overviewRulerLanes: 0,
      scrollbar: { verticalScrollbarSize: 8, horizontalScrollbarSize: 8 },
    });
  });
});

// Read one editor as a param object. Throws with a message naming the field, which the submit
// handler turns into an inline error — the run is never triggered with a half-written payload.
function readJSON(name) {
  const editor = jsonEditors[name];
  if (!editor) throw new Error(`${name}: editor is still loading — try again in a moment.`);
  const raw = editor.getValue().trim();
  if (raw === "") return {};
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch (err) {
    throw new Error(`${name}: ${err.message}`);
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    const got = parsed === null ? "null" : Array.isArray(parsed) ? "an array" : typeof parsed;
    throw new Error(`${name}: must be a JSON object, got ${got}.`);
  }
  return parsed;
}

function showJSONError(message) {
  $$(".json-error").forEach((el) => { el.textContent = ""; });
  if (!message) return;
  // Messages are prefixed with the field name (see readJSON) — route it to that field's slot.
  const el = $(`.json-error[data-json-error="${message.split(":")[0]}"]`);
  if (el) el.textContent = message.slice(message.indexOf(":") + 1).trim();
}

// =============================================================================
// WORKFLOWS
// =============================================================================
function renderWorkflows() {
  $("#wf-count").textContent = workflowList.length;
  const rows = workflowList.map((wf, i) => `<tr>
      <td class="mono">${esc(wf.identifier)}</td>
      <td>${esc(wf.name) || "—"}</td>
      <td>${esc(wf.run_type)}</td>
      <td>${esc(wf.engine_type)}</td>
      <td class="mono">${esc(wf.automation_id)}</td>
      <td class="mono">${esc(wf.ticket_template_id)}</td>
      <td class="actions">
        <button class="btn btn-sm" data-wf-edit="${i}">Edit</button>
        <button class="btn btn-sm" data-wf-view="${i}">View</button>
      </td>
    </tr>`);
  $("#workflows-table tbody").innerHTML = rows.join("") ||
    `<tr class="empty-row"><td colspan="7">No workflows registered yet.</td></tr>`;
}

async function refreshWorkflows() {
  workflowList = await api("GET", "/workflows");
  renderWorkflows();
  populateWorkflowSelect();
}

function populateWorkflowSelect() {
  const sel = $("#trigger-workflow");
  const prev = sel.value;
  sel.innerHTML = workflowList.length
    ? workflowList.map((wf) =>
        `<option value="${esc(wf.identifier)}" data-run-type="${esc(wf.run_type)}">
          ${esc(wf.identifier)} · ${esc(wf.run_type)}</option>`).join("")
    : `<option value="" disabled selected>— register a workflow first —</option>`;
  if (workflowList.some((wf) => wf.identifier === prev)) sel.value = prev;
  toggleResourceFields();
}

function toggleResourceFields() {
  const opt = $("#trigger-workflow").selectedOptions[0];
  const isResource = opt && opt.dataset.runType === "resource";
  $("#resource-fields").hidden = !isResource;
}

// =============================================================================
// RUNS
// =============================================================================
function renderRuns() {
  $("#run-count").textContent = runList.length;
  const rows = runList.map((run, i) => `<tr>
      <td class="mono" title="${esc(run.run_id)}">${esc(run.run_id.slice(0, 8))}…</td>
      <td>${esc(run.run_type)}</td>
      <td>${pill(run.status)}</td>
      <td>${esc(run.current_step) || "—"}</td>
      <td>${esc(run.created_by) || "—"}</td>
      <td class="mono">${esc(run.max_retries)}</td>
      <td class="actions"><button class="btn btn-sm" data-run-view="${i}">View</button></td>
    </tr>`);
  $("#runs-table tbody").innerHTML = rows.join("") ||
    `<tr class="empty-row"><td colspan="7">No runs yet — trigger one.</td></tr>`;
}

async function refreshRuns() {
  runList = await api("GET", "/workflow-runs");
  renderRuns();
}

// =============================================================================
// HEALTH
// =============================================================================
async function checkHealth() {
  const el = $("#health");
  try {
    await api("GET", "/workflows"); // any answered request proves reachability
    el.className = "pill pill-ok";
    el.textContent = "connected";
  } catch {
    el.className = "pill pill-down";
    el.textContent = "unreachable";
  }
}

// A single tick: pull both lists + health. Kept together so one failure
// still lets the others render, and the health pill reflects reachability.
async function tick() {
  await Promise.allSettled([refreshWorkflows(), refreshRuns(), checkHealth()]);
}

// =============================================================================
// FORMS
// =============================================================================
// The register form doubles as the edit form. `editing` holds the identifier
// being edited (null = register mode).
let editing = null;

function openWorkflowForm(wf) {
  const form = $("#register-form");
  editing = wf ? wf.identifier : null;
  form.reset();
  if (wf) {
    form.identifier.value = wf.identifier;
    form.name.value = wf.name || "";
    form.run_type.value = wf.run_type;
    form.engine_type.value = wf.engine_type;
    form.automation_id.value = wf.automation_id;
    form.ticket_template_id.value = wf.ticket_template_id;
  }
  // identifier is the immutable key — lock it while editing.
  $("#wf-identifier").readOnly = !!wf;
  $("#wf-form-title").textContent = wf ? `Edit workflow · ${wf.identifier}` : "Register workflow";
  $("#wf-submit").textContent = wf ? "Save changes" : "Register";
  form.hidden = false;
  (wf ? form.automation_id : $("#wf-identifier")).focus();
}
function closeWorkflowForm() { editing = null; $("#register-form").hidden = true; }

$("#register-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const f = new FormData(e.target);
  const name = (f.get("name") || "").trim();
  const fields = {
    run_type: f.get("run_type"),
    engine_type: f.get("engine_type"),
    automation_id: f.get("automation_id"),
    ticket_template_id: f.get("ticket_template_id"),
    name: name || null,
  };
  try {
    if (editing) {
      const wf = await api("PUT", `/workflows/${encodeURIComponent(editing)}`, fields);
      ok("Workflow updated", `“${wf.identifier}” saved.`);
    } else {
      const body = { identifier: f.get("identifier"), ...fields };
      if (!name) delete body.name;
      const wf = await api("POST", "/workflows", body);
      ok("Workflow registered", `“${wf.identifier}” is ready to trigger.`);
    }
    closeWorkflowForm();
    await refreshWorkflows();
  } catch (err) {
    fail(editing ? "Update failed" : "Register failed", err);
  }
});

$("#trigger-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const f = new FormData(e.target);
  const opt = $("#trigger-workflow").selectedOptions[0];
  if (!opt || !opt.value) return fail("Trigger failed", "Select a workflow first.");

  // Parse every JSON editor before touching the API, so a malformed payload is reported
  // against its own field rather than as a server-side validation error.
  let body;
  try {
    body = {
      workflow_identifier: opt.value,
      created_by: f.get("created_by"),
      max_retries: parseInt(f.get("max_retries"), 10),
      ticket_params: readJSON("ticket_params"),
      workflow_params: readJSON("workflow_params"),
      // Optional: the group whose approval the RITM needs. A name — the orchestrator resolves it
      // to a sys_id. Unrelated to the group a failing DAG names for its incident.
      approval_group: (f.get("approval_group") || "").trim() || null,
    };
    if (opt.dataset.runType === "resource") {
      body.resource = {
        project_id: f.get("res_project_id"),
        resource_type: f.get("res_resource_type"),
        name: f.get("res_name"),
        region: (f.get("res_region") || "").trim() || null,
        environment: (f.get("res_environment") || "").trim() || null,
        description: f.get("res_description") || "",
        tags: csvList(f.get("res_tags")),
        alert_groups: csvList(f.get("res_alert_groups")),
        data: readJSON("res_data"),
      };
      // Only for update/delete of an existing record — a create is assigned the run id server-side.
      const vendorId = (f.get("res_vendor_id") || "").trim();
      if (vendorId) body.resource.vendor_id = vendorId;
    }
    showJSONError(null);
  } catch (err) {
    showJSONError(err.message);
    return fail("Invalid JSON", err);
  }

  try {
    const run = await api("POST", "/workflow-runs", body);
    ok("Run triggered", `Run ${run.run_id.slice(0, 8)}… is ${run.status}.`);
    e.target.hidden = true;
    await refreshRuns();
    selectTab("runs");
  } catch (err) {
    fail("Trigger failed", err);
  }
});

// =============================================================================
// TABS
// =============================================================================
function selectTab(name) {
  $$(".tab").forEach((t) => t.classList.toggle("is-active", t.dataset.tab === name));
  $$(".tabpanel").forEach((p) => p.classList.toggle("is-active", p.id === `tab-${name}`));
}

// =============================================================================
// EVENTS
// =============================================================================
$("#trigger-workflow").addEventListener("change", toggleResourceFields);

document.addEventListener("click", async (e) => {
  const t = e.target.closest(
    "[data-tab],[data-act],[data-wf-view],[data-wf-edit],[data-run-view],[data-json-format]");
  if (!t) return;
  const d = t.dataset;

  if (d.tab) return selectTab(d.tab);

  // Same as Shift+Alt+F in the editor, surfaced as a button for discoverability.
  if (d.jsonFormat) {
    const editor = jsonEditors[d.jsonFormat];
    return void (editor && editor.getAction("editor.action.formatDocument").run());
  }

  try {
    if (d.act === "new-workflow") openWorkflowForm(null);
    else if (d.act === "cancel-workflow") closeWorkflowForm();
    else if (d.wfEdit !== undefined) openWorkflowForm(workflowList[d.wfEdit]);
    else if (d.wfView !== undefined) openDrawer(
      `Workflow · ${workflowList[d.wfView].identifier}`, workflowList[d.wfView]);

    else if (d.act === "new-run") {
      if (!workflowList.length) return fail("No workflows", "Register a workflow before triggering a run.");
      $("#trigger-form").hidden = false;
      $("#trigger-form input[name=created_by]").focus();
    }
    else if (d.act === "cancel-run") { $("#trigger-form").hidden = true; }
    else if (d.runView !== undefined) openDrawer(
      `Run · ${runList[d.runView].run_id}`, runList[d.runView]);

    else if (d.act === "close-drawer") closeDrawer();
  } catch (err) {
    fail("Operation failed", err);
  }
});

// =============================================================================
// BOOT
// =============================================================================
renderWorkflows();
renderRuns();
tick();
setInterval(tick, REFRESH_MS);
