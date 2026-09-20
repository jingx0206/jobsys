'use strict';

// jobsys web UI. Lists jobs and runs, creates and triggers jobs, and follows a
// run's log live over server-sent events. It talks to the API on the same
// origin and polls for status changes every couple of seconds.

const POLL_MS = 2000;
const $ = (selector) => document.querySelector(selector);

const state = {
  jobs: new Map(), // job id -> job
  filter: '', // job id the runs table shows, or '' for all
  selected: null, // run id in the detail panel
  stream: null, // EventSource following the selected run
  streamEnded: false,
};

async function api(path, options = {}) {
  const res = await fetch(path, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(body.error || `${res.status} ${res.statusText}`);
  return body;
}

// el builds a DOM node. Text always goes through textContent, never HTML.
function el(tag, props = {}, ...children) {
  const node = document.createElement(tag);
  Object.assign(node, props);
  node.append(...children);
  return node;
}

function relative(iso) {
  if (!iso) return '–';
  const secs = Math.round((new Date(iso) - Date.now()) / 1000);
  const abs = Math.abs(secs);
  const text =
    abs < 60 ? `${abs}s`
    : abs < 3600 ? `${Math.round(abs / 60)}m`
    : abs < 86400 ? `${Math.round(abs / 3600)}h`
    : `${Math.round(abs / 86400)}d`;
  return secs > 0 ? `in ${text}` : `${text} ago`;
}

function localTime(iso) {
  return iso ? new Date(iso).toLocaleString() : '–';
}

function took(run) {
  if (!run.started_at) return '–';
  const end = run.finished_at ? new Date(run.finished_at) : new Date();
  const secs = (end - new Date(run.started_at)) / 1000;
  return secs < 60 ? `${secs.toFixed(1)}s` : `${Math.floor(secs / 60)}m ${Math.round(secs % 60)}s`;
}

// The tables only re-render when their data changes, so rows don't jump,
// flicker or swallow a click every poll. Relative times, which do change every
// poll, are refreshed in place by refreshTimes.
const rendered = { jobs: '', runs: '' };

function timeCell(iso) {
  const td = el('td', { textContent: relative(iso), title: localTime(iso) });
  if (iso) td.dataset.time = iso;
  return td;
}

function tookCell(run) {
  const td = el('td', { className: 'num', textContent: took(run) });
  if (run.started_at && !run.finished_at) td.dataset.started = run.started_at; // still running
  return td;
}

function refreshTimes() {
  for (const td of document.querySelectorAll('td[data-time]')) {
    td.textContent = relative(td.dataset.time);
  }
  for (const td of document.querySelectorAll('td[data-started]')) {
    td.textContent = took({ started_at: td.dataset.started });
  }
}

function badge(status) {
  return el('span', { className: `badge ${status.toLowerCase()}`, textContent: status });
}

function jobName(id) {
  return state.jobs.get(id)?.name ?? id.slice(0, 8);
}

let flashTimer;
function flash(message, kind = 'error') {
  const box = $('#flash');
  box.textContent = message;
  box.className = `flash ${kind}`;
  box.hidden = false;
  clearTimeout(flashTimer);
  flashTimer = setTimeout(() => (box.hidden = true), 5000);
}

function setConnected(ok, detail = '') {
  const conn = $('#conn');
  conn.className = `conn ${ok ? 'ok' : 'down'}`;
  conn.textContent = ok ? 'live' : 'API unreachable';
  conn.title = detail;
}

// ---- jobs ----

async function loadJobs() {
  const { jobs } = await api('/jobs?limit=200');
  state.jobs = new Map(jobs.map((j) => [j.id, j]));

  const key = JSON.stringify([jobs, state.filter]);
  if (key === rendered.jobs) return;
  rendered.jobs = key;

  $('#jobs tbody').replaceChildren(...jobs.map((j) => {
    const runButton = el('button', { className: 'small', textContent: 'Run' });
    runButton.addEventListener('click', (e) => {
      e.stopPropagation();
      runJob(j.id, runButton);
    });
    const buttons = [runButton];
    if (j.cron_expr) {
      const toggle = el('button', { className: 'small secondary', textContent: j.enabled ? 'Pause' : 'Resume' });
      toggle.addEventListener('click', (e) => {
        e.stopPropagation();
        setEnabled(j, !j.enabled, toggle);
      });
      buttons.unshift(toggle);
    }
    const schedule = j.cron_expr
      ? `${j.cron_expr}${j.timezone && j.timezone !== 'UTC' ? ` (${j.timezone})` : ''}`
      : 'manual';
    const next = j.cron_expr && !j.enabled
      ? el('td', { className: 'dim', textContent: 'paused' })
      : timeCell(j.next_run_at);
    const row = el('tr', { className: state.filter === j.id ? 'selected' : '', title: 'Show only this job’s runs' },
      el('td', { textContent: j.name }),
      el('td', { className: j.cron_expr ? 'mono' : 'dim', textContent: schedule }),
      next,
      el('td', { className: 'right buttons' }, ...buttons));
    row.addEventListener('click', () => setFilter(state.filter === j.id ? '' : j.id));
    return row;
  }));
  $('#jobs-empty').hidden = jobs.length > 0;

  const select = $('#job-filter');
  select.replaceChildren(
    el('option', { value: '', textContent: 'All jobs' }),
    ...jobs.map((j) => el('option', { value: j.id, textContent: j.name })));
  select.value = state.filter;
}

function setFilter(jobId) {
  state.filter = jobId;
  refresh();
}

async function runJob(jobId, button) {
  button.disabled = true;
  try {
    const run = await api(`/jobs/${jobId}/run`, { method: 'POST' });
    openRun(run.id);
    await loadRuns();
  } catch (err) {
    flash(`Could not start the run: ${err.message}`);
  } finally {
    button.disabled = false;
  }
}

async function setEnabled(job, enabled, button) {
  button.disabled = true;
  try {
    await api(`/jobs/${job.id}/${enabled ? 'resume' : 'pause'}`, { method: 'POST' });
    flash(`${enabled ? 'Resumed' : 'Paused'} ${job.name}.`, 'ok');
    await loadJobs();
  } catch (err) {
    flash(`Could not ${enabled ? 'resume' : 'pause'} ${job.name}: ${err.message}`);
    button.disabled = false;
  }
}

$('#toggle-create').addEventListener('click', () => {
  const form = $('#create-form');
  form.hidden = !form.hidden;
  if (!form.hidden) form.elements.name.focus();
});

$('#create-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.target;
  const f = form.elements;
  const num = (name) => (f[name].value === '' ? undefined : Number(f[name].value));

  const payload = { lines: num('lines') };
  for (const key of ['delay_ms', 'fail_at', 'fail_attempts']) {
    if (num(key)) payload[key] = num(key);
  }
  if (f.prefix.value) payload.prefix = f.prefix.value;

  const body = { name: f.name.value.trim(), type: 'generate_text', payload };
  if (f.cron_expr.value.trim()) body.cron_expr = f.cron_expr.value.trim();
  if (f.timezone.value.trim()) body.timezone = f.timezone.value.trim();
  if (num('max_retries') !== undefined) body.max_retries = num('max_retries');
  if (num('timeout_sec') !== undefined) body.timeout_sec = num('timeout_sec');

  const errorBox = $('#create-error');
  errorBox.hidden = true;
  try {
    const job = await api('/jobs', { method: 'POST', body: JSON.stringify(body) });
    form.reset();
    form.hidden = true;
    flash(`Created ${job.name}${job.next_run_at ? `; first run ${relative(job.next_run_at)}` : ''}.`, 'ok');
    await loadJobs();
  } catch (err) {
    errorBox.textContent = err.message;
    errorBox.hidden = false;
  }
});

// ---- runs ----

async function loadRuns() {
  const query = state.filter ? `&job_id=${state.filter}` : '';
  const { runs } = await api(`/runs?limit=50${query}`);

  // Heartbeats change a running run every few seconds; leave them out so they
  // don't force a re-render.
  const key = JSON.stringify([state.selected, runs.map((r) =>
    [r.id, r.status, r.attempt, r.worker_id, r.started_at, r.finished_at])]);
  if (key === rendered.runs) return;
  rendered.runs = key;

  $('#runs tbody').replaceChildren(...runs.map((r) => {
    const row = el('tr', { className: r.id === state.selected ? 'selected' : '' },
      el('td', {}, badge(r.status)),
      el('td', { textContent: jobName(r.job_id) }),
      el('td', { className: 'dim', textContent: r.trigger }),
      el('td', { className: 'num', textContent: r.attempt }),
      el('td', { className: 'mono dim', textContent: r.worker_id ?? '–' }),
      timeCell(r.scheduled_time),
      tookCell(r));
    row.addEventListener('click', () => openRun(r.id));
    return row;
  }));
  $('#runs-empty').hidden = runs.length > 0;
}

// ---- run detail and live log ----

function openRun(runId) {
  state.stream?.close();
  state.selected = runId;
  state.streamEnded = false;
  $('#detail').hidden = false;
  $('#d-id').textContent = runId.slice(0, 8);
  $('#d-id').title = runId;
  $('#d-log').replaceChildren();
  $('#d-output').hidden = $('#d-output-head').hidden = true;
  loadRunMeta(runId).catch((err) => flash(err.message));
  follow(runId);
  $('#detail').scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function follow(runId) {
  setStreamState('live', 'following');
  const es = new EventSource(`/runs/${runId}/logs/stream`);
  state.stream = es;

  es.addEventListener('attempt', (e) => {
    const { attempt } = JSON.parse(e.data);
    appendLog(el('div', { className: 'attempt', textContent: `attempt ${attempt}` }));
  });
  es.addEventListener('line', (e) => {
    const l = JSON.parse(e.data);
    appendLog(el('div', { className: `line ${l.level}` },
      el('span', { className: 'ts', textContent: new Date(l.ts).toLocaleTimeString() }),
      el('span', { className: 'lvl', textContent: l.level }),
      el('span', { textContent: l.line })));
  });
  es.addEventListener('end', (e) => {
    const { status } = JSON.parse(e.data);
    state.streamEnded = true;
    es.close();
    setStreamState(status.toLowerCase(), `finished: ${status}`);
    loadRunMeta(runId).catch(() => {});
  });
  es.onerror = () => {
    if (state.streamEnded || state.stream !== es) return;
    // The stream restarts from attempt 1, so don't let the browser reconnect
    // on its own and duplicate lines; offer a clean reconnect instead.
    es.close();
    setStreamState('failed', 'disconnected');
    const retry = el('button', { className: 'small secondary', textContent: 'Reconnect' });
    retry.addEventListener('click', () => {
      $('#d-log').replaceChildren();
      follow(runId);
    });
    $('#d-stream').append(' ', retry);
  };
}

function appendLog(node) {
  const log = $('#d-log');
  const atBottom = log.scrollHeight - log.scrollTop - log.clientHeight < 40;
  log.append(node);
  if (atBottom) log.scrollTop = log.scrollHeight;
}

function setStreamState(kind, text) {
  const s = $('#d-stream');
  s.className = `stream-state ${kind}`;
  s.textContent = text;
}

async function loadRunMeta(runId) {
  const r = await api(`/runs/${runId}`);
  if (state.selected !== runId) return;

  const rows = [
    ['Status', badge(r.status)],
    ['Job', jobName(r.job_id)],
    ['Trigger', r.trigger],
    ['Attempt', String(r.attempt)],
    ['Worker', r.worker_id ?? '–'],
    ['Scheduled', localTime(r.scheduled_time)],
    ['Started', localTime(r.started_at)],
    ['Finished', localTime(r.finished_at)],
  ];
  if (r.next_attempt_at) {
    rows.push(['Retry at', `${localTime(r.next_attempt_at)} (${relative(r.next_attempt_at)})`]);
  }
  if (r.error) rows.push(['Error', r.error]);
  $('#d-meta').replaceChildren(...rows.flatMap(([k, v]) => [el('dt', { textContent: k }), el('dd', {}, v)]));

  const out = $('#d-output');
  if (r.output) {
    const lines = r.output.split('\n');
    const more = lines.length - 201; // the output ends with a newline
    out.textContent = more > 0 ? `${lines.slice(0, 200).join('\n')}\n… ${more} more lines` : r.output;
  }
  out.hidden = $('#d-output-head').hidden = !r.output;
}

$('#job-filter').addEventListener('change', (e) => setFilter(e.target.value));
$('#d-close').addEventListener('click', () => {
  state.stream?.close();
  state.selected = null;
  $('#detail').hidden = true;
  loadRuns().catch(() => {});
});

// ---- polling ----

async function refresh() {
  try {
    await loadJobs();
    await loadRuns();
    if (state.selected && !state.streamEnded) await loadRunMeta(state.selected);
    refreshTimes();
    setConnected(true);
  } catch (err) {
    setConnected(false, err.message);
  }
}

refresh();
setInterval(refresh, POLL_MS);
