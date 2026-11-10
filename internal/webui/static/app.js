const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => [...document.querySelectorAll(selector)];

const state = {
  metrics: null,
  activity: Array(24).fill(0),
  guideStep: 0,
  busy: false,
};

const elements = {
  connection: $('#connectionBadge'),
  responseStatus: $('#responseStatus'),
  responseTime: $('#responseTime'),
  responseBody: $('#responseBody'),
  role: $('#metricRole'), node: $('#metricNode'), term: $('#metricTerm'),
  leader: $('#metricLeader'), commit: $('#metricCommit'), lag: $('#metricLag'),
  stageMode: $('#stageMode'), heroTerm: $('#heroTerm'), heroCommit: $('#heroCommit'),
  heroApplied: $('#heroApplied'), nodeOneRole: $('#nodeOneRole'), requestTotal: $('#requestTotal'),
  chart: $('#activityChart polyline'), chartFill: $('#activityChart .chart-fill'), toast: $('#toast'),
};

function setConnection(mode, label) {
  elements.connection.className = `connection ${mode}`;
  elements.connection.lastChild.textContent = label;
}

async function refreshMetrics() {
  try {
    const response = await fetch('/metrics', { headers: { Accept: 'application/json' }, cache: 'no-store' });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const report = await response.json();
    state.metrics = report;
    renderMetrics(report);
    setConnection('online', 'Live backend');
  } catch (error) {
    setConnection('offline', 'Backend unavailable');
    elements.stageMode.textContent = 'offline';
    elements.role.textContent = 'OFFLINE';
  }
}

function renderMetrics(report) {
  const raft = report.raft;
  const total = (report.requests || []).reduce((sum, metric) => sum + metric.requests, 0);
  state.activity.push(total);
  state.activity.shift();
  renderChart();
  elements.requestTotal.textContent = `${total} request${total === 1 ? '' : 's'}`;

  if (!raft) {
    elements.role.textContent = 'STANDALONE';
    elements.node.textContent = 'single node';
    elements.term.textContent = '—'; elements.leader.textContent = 'local';
    elements.commit.textContent = 'WAL'; elements.lag.textContent = '0';
    elements.stageMode.textContent = 'standalone / durable WAL';
    elements.nodeOneRole.textContent = 'PRIMARY';
    elements.heroTerm.textContent = '—'; elements.heroCommit.textContent = 'WAL'; elements.heroApplied.textContent = 'LIVE';
    $('.node-two').classList.add('muted'); $('.node-three').classList.add('muted');
    return;
  }

  const role = String(raft.role || 'unknown').toUpperCase();
  elements.role.textContent = role;
  elements.node.textContent = raft.node_id || '—';
  elements.term.textContent = raft.term ?? '—';
  elements.leader.textContent = raft.leader_id || (role === 'LEADER' ? raft.node_id : 'electing');
  elements.commit.textContent = raft.commit_index ?? '—';
  elements.lag.textContent = raft.apply_lag ?? '—';
  elements.stageMode.textContent = `${role.toLowerCase()} / ${raft.node_id}`;
  elements.nodeOneRole.textContent = role;
  elements.heroTerm.textContent = raft.term ?? '—';
  elements.heroCommit.textContent = raft.commit_index ?? '—';
  elements.heroApplied.textContent = raft.last_applied ?? '—';
  $('#roleDot').style.background = role === 'LEADER' ? 'var(--acid)' : role === 'FOLLOWER' ? 'var(--cyan)' : 'var(--orange)';

  const peers = raft.peer_replication || [];
  $$('.node.muted').forEach((node, index) => {
    const peer = peers[index];
    node.classList.toggle('muted', !peer || peer.lag > 0);
    const label = node.querySelector('small');
    label.textContent = peer ? `LAG ${peer.lag}` : 'PEER';
  });
}

function renderChart() {
  const max = Math.max(...state.activity, 1);
  const points = state.activity.map((value, index) => `${(index / (state.activity.length - 1)) * 360},${86 - (value / max) * 72}`).join(' ');
  elements.chart.setAttribute('points', points);
  elements.chartFill.setAttribute('d', `M0 90 L${points.replaceAll(' ', ' L')} L360 90 Z`);
}

async function performOperation(operation) {
  if (state.busy) return;
  const key = $('#keyInput').value.trim();
  const value = $('#valueInput').value;
  if (!key) { showToast('Enter a key first.'); $('#keyInput').focus(); return; }
  state.busy = true;
  elements.responseStatus.textContent = 'RUNNING';
  elements.responseStatus.style.color = 'var(--orange)';
  elements.responseBody.textContent = `${operation.toUpperCase()} /v1/keys/${key}`;
  const started = performance.now();
  try {
    const options = { method: operation.toUpperCase(), headers: { Accept: 'application/json, application/octet-stream' } };
    if (operation === 'put') options.body = value;
    const response = await fetch(`/v1/keys/${encodeURIComponent(key)}`, options);
    const contentType = response.headers.get('content-type') || '';
    const text = await response.text();
    let body = text;
    if (contentType.includes('application/json') && text) {
      try { body = JSON.stringify(JSON.parse(text), null, 2); } catch (_) { /* display raw response */ }
    }
    const elapsed = performance.now() - started;
    elements.responseStatus.textContent = `${response.status} ${response.statusText || statusLabel(response.status)}`;
    elements.responseStatus.style.color = response.ok ? 'var(--acid)' : 'var(--red)';
    elements.responseTime.textContent = `${elapsed.toFixed(1)} ms`;
    elements.responseBody.textContent = body || (response.status === 204 ? 'Success. The mutation was committed and applied.' : 'Empty response.');
    if (response.ok) advanceGuide(operation);
    if (response.status === 503 && body.includes('leader_id')) showToast('This node is a follower. Open the UI on the leader node shown in the response.');
  } catch (error) {
    elements.responseStatus.textContent = 'NETWORK ERROR';
    elements.responseStatus.style.color = 'var(--red)';
    elements.responseTime.textContent = `${(performance.now() - started).toFixed(1)} ms`;
    elements.responseBody.textContent = error.message;
  } finally {
    state.busy = false;
    await refreshMetrics();
  }
}

function statusLabel(status) {
  return ({ 204: 'No Content', 404: 'Not Found', 503: 'Not Leader', 504: 'Timeout' })[status] || '';
}

function advanceGuide(operation) {
  const expected = ['put', 'get', 'delete'];
  if (operation === expected[state.guideStep]) state.guideStep += 1;
  $$('.guide-steps > div').forEach((step, index) => step.classList.toggle('done', index <= state.guideStep));
  if (state.guideStep === 3) showToast('Workflow complete: write, linearizable read, and delete verified.');
}

let toastTimer;
function showToast(message) {
  clearTimeout(toastTimer);
  elements.toast.textContent = message;
  elements.toast.classList.add('show');
  toastTimer = setTimeout(() => elements.toast.classList.remove('show'), 4200);
}

function setupInteractions() {
  $('#operationForm').addEventListener('submit', (event) => { event.preventDefault(); performOperation('put'); });
  $$('[data-operation="get"], [data-operation="delete"]').forEach((button) => button.addEventListener('click', () => performOperation(button.dataset.operation)));
  $('#refreshButton').addEventListener('click', refreshMetrics);

  const observer = new IntersectionObserver((entries) => {
    entries.forEach((entry) => { if (entry.isIntersecting) entry.target.classList.add('visible'); });
  }, { threshold: .12 });
  $$('.reveal').forEach((element) => observer.observe(element));
}

setupInteractions();
renderChart();
refreshMetrics();
setInterval(refreshMetrics, 2500);
