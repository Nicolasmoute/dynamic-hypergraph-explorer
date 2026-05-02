// =========================================================================
// Dynamic Hypergraph Explorer — Lightweight Client
// All heavy computation runs on the FastAPI server.
// =========================================================================

// \u00a76.5 [M5] XSS guard \u2014 escape user-controlled strings before innerHTML injection
function escHtml(str) {
  return String(str == null ? '' : str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// \u00a76.8 [L5] BLURBS removed \u2014 rule descriptions now come from server /api/rules blurb field

// Feature flag: ?renderer=canvas enables the Canvas 2D path (Phase 2/3).
// Default (no param or ?renderer=svg) keeps the existing SVG path unchanged.
const USE_CANVAS = new URLSearchParams(location.search).get('renderer') === 'canvas';

let RULES = [];        // [{id, name, notation, desc, tag, tagClass}]
let DATA = {};         // ruleId -> {states, events, causal_edges, stats, lineage, birthSteps}
let MULTIWAY = {};     // ruleId -> server multiway data
let activeRule = null;
let currentStep = 0;
let currentView = 'spatial';
let playing = false;
let playTimer = null;
let playIntervalMs = 1200; // §6.9 [L6] configurable play speed (50–5000 ms)

// =========================================================================
// COMPUTE LIVENESS — overlay state machine
// =========================================================================
const _computeStart = {};   // ruleId → timestamp when fetch started
const _computeState = {};   // ruleId → 'running' | 'stale' | 'cached' | 'error'
let   _computeTimer  = null;
// Stale detection: server-derived for custom jobs (pollJobUntilDone);
// built-in rules rely on the 30s AbortController timeout in apiFetch.

function showComputeOverlay(state, msg) {
  const el = document.getElementById('compute-overlay');
  if (!el) return;
  el.className = 'visible' + (state === 'stale' ? ' stale' : state === 'error' ? ' error' : '');
  document.getElementById('co-msg').textContent = msg || 'Server is computing…';
  document.getElementById('co-elapsed').textContent = '';
}

function hideComputeOverlay() {
  const el = document.getElementById('compute-overlay');
  if (el) el.className = '';
}

function _tickComputeElapsed(ruleId) {
  if (activeRule !== ruleId || _computeState[ruleId] === 'cached') return;
  const elapsed = Math.floor((Date.now() - (_computeStart[ruleId] || Date.now())) / 1000);
  const elEl = document.getElementById('co-elapsed');
  if (elEl) elEl.textContent = elapsed + 's elapsed';
  // Stale promoted by server via pollJobUntilDone.
}

function startComputeTimer(ruleId) {
  stopComputeTimer();
  if (!_computeStart[ruleId]) _computeStart[ruleId] = Date.now();
  _computeTimer = setInterval(() => _tickComputeElapsed(ruleId), 1000);
}

function stopComputeTimer() {
  if (_computeTimer) { clearInterval(_computeTimer); _computeTimer = null; }
}

function retryActiveRule() {
  if (!activeRule) return;
  delete DATA[activeRule];
  delete _computeState[activeRule];
  delete _computeStart[activeRule];
  selectRule(activeRule);
}

// promise-based delay
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── Force-layout live controls ─────────────────────────────────────────────
function applyForceParams() {
  if (!simulation) return;
  const n = Math.max(simulation.nodes().length, 1);
  const baseDist = 20 + 200 / Math.sqrt(n);
  simulation.force('link')
    .distance(d => ((selectedEdges.length > 0 && getEdgeSelColor(d.edgeIdx)) ? baseDist * 0.2 : baseDist) * forceParams.linkDistMult)
    .strength(d => (selectedEdges.length > 0 && getEdgeSelColor(d.edgeIdx)) ? 1.0 : forceParams.linkStrength);
  simulation.force('charge')
    .strength((-30 - 2000 / n) * forceParams.chargeMult);
  simulation.alpha(0.3).restart();
}
function setForceParam(key, val) {
  forceParams[key] = val;
  const labels = { linkDistMult: 'fp-dist-val', chargeMult: 'fp-charge-val', linkStrength: 'fp-strength-val' };
  const el = document.getElementById(labels[key]);
  if (el) el.textContent = val.toFixed(2);
  applyForceParams();
}
function resetForceParams() {
  forceParams = { linkDistMult: 1.0, chargeMult: 1.0, linkStrength: 0.3 };
  const map = { 'fp-dist': 1, 'fp-charge': 1, 'fp-strength': 0.3,
                'fp-dist-val': '1.00', 'fp-charge-val': '1.00', 'fp-strength-val': '0.30' };
  Object.entries(map).forEach(([id, v]) => {
    const el = document.getElementById(id);
    if (el) typeof v === 'string' ? (el.textContent = v) : (el.value = v);
  });
  applyForceParams();
}
// ──────────────────────────────────────────────────────────────────────────

// ── Extend +1 step & abort ────────────────────────────────────────────────
function updateExtendRow() {
  const row = document.getElementById('extend-row');
  if (!row) return;
  // Show whenever data is loaded and we have a cache key (built-in or custom).
  const hasKey = activeRule && DATA[activeRule] && DATA[activeRule]._cacheKey
                 && !DATA[activeRule]._error;
  row.style.display = hasKey ? 'flex' : 'none';
}

async function extendOneStep() {
  const d = DATA[activeRule];
  if (!activeRule || !d || !d._cacheKey) return;
  const btn = document.getElementById('extend-btn');
  const statusEl = document.getElementById('extend-status');
  btn.disabled = true;
  if (statusEl) statusEl.textContent = '';
  _jobAborted = false; // reset before starting a new job

  showComputeOverlay('running', 'Extending by 1 step…');
  startComputeTimer(activeRule);

  try {
    // POST /api/extend — body: {key, extra_steps:1}
    // Returns same job shape as POST /api/custom (status:'done' or 'running'+job_id).
    // Done response is the FULL evolution (replace, not append).
    const resp = await apiFetch('/api/extend', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ key: d._cacheKey, extra_steps: 1 }),
    });

    let result;
    if (resp.status === 'done') {
      result = resp;
    } else if (resp.job_id) {
      _currentJobId = resp.job_id;
      const finalPoll = await pollJobUntilDone(resp.job_id, poll => {
        const msg = poll.elapsed_s != null
          ? 'Extending… ' + poll.elapsed_s.toFixed(1) + 's'
          : 'Extending…';
        document.getElementById('co-msg').textContent = msg;
      });
      _currentJobId = null;
      if (finalPoll.status !== 'done') {
        stopComputeTimer();
        if (_jobAborted) {
          // abortCurrentJob() already updated the overlay — just re-enable button.
          _jobAborted = false;
          btn.disabled = false;
          return;
        }
        showComputeOverlay(
          finalPoll.status === 'stale' ? 'stale' : 'error',
          finalPoll.status === 'stale'
            ? 'Server restarted mid-extend — click Retry'
            : 'Extend failed: ' + (finalPoll.error || 'server error')
        );
        btn.disabled = false;
        return;
      }
      result = finalPoll;
    } else {
      throw new Error('Unexpected response from /api/extend');
    }

    // Full replace — server merges causal edges correctly server-side.
    d.states       = result.states       || d.states;
    d.events       = result.events       || d.events;
    d.causal_edges = result.causalEdges  || d.causal_edges;
    d.stats        = result.stats        || d.stats;
    d.lineage      = result.lineage      || d.lineage;
    d.birthSteps   = result.birthSteps   || d.birthSteps;
    // Update cache key for the next extend call (server returns new key).
    if (result.key) d._cacheKey = result.key;
    if (result.multiway) MULTIWAY[activeRule] = result.multiway;

    const newMax = (d.states || []).length - 1;
    const slider = document.getElementById('step-slider');
    if (slider) { slider.max = newMax; slider.value = newMax; }
    currentStep = newMax;

    stopComputeTimer();
    hideComputeOverlay();
    updateExtendRow();
    renderCurrentView();
  } catch (e) {
    stopComputeTimer();
    showComputeOverlay('error', 'Extend failed: ' + e.message);
  } finally {
    btn.disabled = false;
    _currentJobId = null;
  }
}

async function abortCurrentJob() {
  const jobId = _currentJobId;
  if (!jobId) return;
  _jobAborted   = true;  // tell callers to skip their own error overlay
  _currentJobId = null;
  stopComputeTimer();
  showComputeOverlay('error', 'Computation aborted — click Retry to restart');
  // Fire-and-forget DELETE — UI already updated, don't block on the response.
  apiFetch('/api/jobs/' + jobId, { method: 'DELETE' }).catch(e => {
    // 409 (already terminal) or 404 (not found) — expected, treat as no-op.
    const is409or404 = e.message && (e.message.includes('409') || e.message.includes('404'));
    if (!is409or404) console.warn('abort error:', e.message);
  });
}
// ──────────────────────────────────────────────────────────────────────────

// Poll GET /api/jobs/{job_id} every 2s (Ada's contract, t-2026-04-30-b365ed66).
// Calls onProgress(poll) on each response. Returns terminal response
// (status = done | failed | stale). Network errors are retried silently.
async function pollJobUntilDone(jobId, onProgress) {
  while (true) {
    await sleep(2000);
    let poll;
    try {
      poll = await apiFetch('/api/jobs/' + jobId);
    } catch (e) {
      continue; // transient network error — retry
    }
    if (onProgress) onProgress(poll);
    if (poll.status !== 'running') return poll;
  }
}
let simulation = null;
let isDark = true;
let _currentJobId = null;  // job being polled — set so abortCurrentJob() can cancel it
let _jobAborted   = false; // set by abortCurrentJob(); callers skip their error overlay

// Force-layout user controls (live-adjustable via sidebar sliders)
let forceParams = { linkDistMult: 1.0, chargeMult: 1.0, linkStrength: 0.3 };
let _prevNodePositions = new Map(); // warmstart: reuse positions across step changes

// Options
let opts = { labels: false, colors: true, hulls: true, nudge: false };

// Lineage tracking — multi-select up to 3 edges
const SEL_COLORS = ['#ffdd00', '#ff2222', '#2288ff'];
const SEL_MIX = {
  '100': '#ffdd00', '010': '#ff2222', '001': '#2288ff',
  '110': '#ff8800', '101': '#44dd44', '011': '#cc44ff',
  '111': '#ffffff'
};
let selectedEdges = [];
let lineageSets = [];

function getEdgeSelColor(edgeIdx) {
  const bits = [0, 0, 0];
  for (let i = 0; i < selectedEdges.length; i++) {
    const sel = selectedEdges[i];
    const isOrigin = currentStep === sel.step && edgeIdx === sel.edgeIdx;
    const isDesc = lineageSets[i] && lineageSets[i].has(edgeIdx);
    if (isOrigin || isDesc) bits[i] = 1;
  }
  const key = bits.join('');
  if (key === '000') return null;
  return SEL_MIX[key] || null;
}

// Multiway selection
let selectedMultiwayNode = null;
let selectedPath = null;

// Palettes
const paletteDark = [
  '#6c7bff','#ff6b9d','#4cdd8a','#ffaa44','#cb7cff',
  '#4cdddd','#ff6b6b','#b8ff7c','#ff7cff','#7cfff0',
  '#ff9f43','#a29bfe','#fd79a8','#00cec9','#e17055'
];
const paletteLight = [
  '#2b3ab8','#b82060','#0a7a3a','#a85800','#7a2ab8',
  '#0a7a7a','#c02020','#3a7a0a','#a020a0','#0a8a6a',
  '#b86a00','#4a3ab8','#c0286a','#087a7a','#b84a2a'
];
function getPalette() { return isDark ? paletteDark : paletteLight; }

// =========================================================================
// THEME
// =========================================================================
function toggleTheme() {
  isDark = !isDark;
  document.documentElement.classList.toggle('light', !isDark);
  document.getElementById('theme-btn').innerHTML = isDark ? '&#9790;' : '&#9728;';
  renderCurrentView();
}

// =========================================================================
// API helpers
// =========================================================================
// §6.10 [L7] AbortController timeout on apiFetch (30 s default)
async function apiFetch(path, options = {}, timeoutMs = 30000) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const resp = await fetch(path, { ...options, signal: ctrl.signal });
    if (!resp.ok) throw new Error(`API error ${resp.status}: ${await resp.text()}`);
    return resp.json();
  } catch (e) {
    if (e.name === 'AbortError') throw new Error(`Request timed out after ${timeoutMs / 1000}s — click the rule card to retry`);
    throw e;
  } finally {
    clearTimeout(timer);
  }
}

// =========================================================================
// INIT
// =========================================================================
async function init() {
  const overlay = document.getElementById('loading-overlay');
  const loadingText = document.getElementById('loading-text');

  try {
    loadingText.textContent = 'Loading rules...';
    RULES = await apiFetch('/api/rules');

    // Show UI immediately with rule cards
    overlay.style.display = 'none';
    renderRuleCards();

    // Start loading first rule (shows per-rule loading in canvas area)
    selectRule(RULES[0].id);
  } catch (e) {
    loadingText.textContent = 'Error: ' + e.message;
    console.error('Init failed:', e);
  }
}

// Track loading state per rule
const _loading = {};

async function loadRuleData(ruleId) {
  if (DATA[ruleId]) return DATA[ruleId];
  if (_loading[ruleId]) return _loading[ruleId];

  // Track compute start (may already be set if selectRule showed the overlay)
  if (!_computeStart[ruleId]) _computeStart[ruleId] = Date.now();
  _computeState[ruleId] = 'running';

  _loading[ruleId] = (async () => {
    try {
      const ruleData = await apiFetch('/api/rules/' + ruleId);
      DATA[ruleId] = {
        states: ruleData.states,
        events: ruleData.events,
        causal_edges: ruleData.causalEdges,
        stats: ruleData.stats,
        lineage: ruleData.lineage,
        birthSteps: ruleData.birthSteps,
        // Built-in rules use their rule ID as the cache key for /api/extend.
        _cacheKey: ruleData.key || ruleId,
      };
      _computeState[ruleId] = 'cached';
      if (activeRule === ruleId) {
        stopComputeTimer();
        hideComputeOverlay();
        // Mirror the sidebar setup that selectRule skipped on early-return:
        // slider range, blurb text, and multiway background load.
        const _maxStep = (DATA[ruleId].states || []).length - 1;
        const _slider = document.getElementById('step-slider');
        if (_slider) {
          _slider.max = _maxStep;
          _slider.value = Math.min(currentStep, _maxStep);
          currentStep = +_slider.value;
        }
        const _rule = RULES.find(r => r.id === ruleId);
        const _blurbEl = document.getElementById('theory-blurb');
        if (_blurbEl) _blurbEl.textContent = (_rule && _rule.blurb) || '';
        if (!MULTIWAY[ruleId] && !ruleId.startsWith('custom_')) loadMultiway(ruleId);
        updateExtendRow();
        renderCurrentView();
      }
      return DATA[ruleId];
    } catch (e) {
      console.warn('Failed to load rule', ruleId, e);
      DATA[ruleId] = { _error: true, _errorMsg: e.message };
      _computeState[ruleId] = 'error';
      if (activeRule === ruleId) {
        stopComputeTimer();
        showComputeOverlay('error',
          'Failed to load: ' + (e.message || 'server error') + ' — click Retry');
      }
      return null;
    } finally {
      delete _loading[ruleId];
    }
  })();
  return _loading[ruleId];
}

async function loadMultiway(ruleId) {
  if (MULTIWAY[ruleId]) return;
  try {
    MULTIWAY[ruleId] = await apiFetch('/api/rules/' + ruleId + '/multiway');
    if (activeRule === ruleId && currentView === 'multiway') renderMultiway();
  } catch (e) {
    console.warn('Failed to load multiway for', ruleId, e);
  }
}

// =========================================================================
// LINEAGE — uses server-provided lineage map
// =========================================================================
function getDescendantsFromStep(ruleId, viewingStep, edgeIdx, originStep) {
  if (originStep === undefined) originStep = viewingStep;
  const data = DATA[ruleId];
  if (!data || !data.lineage) return new Set();

  const result = new Set();
  const queue = [originStep + ':' + edgeIdx];
  const visited = new Set(queue);

  let head = 0; // cursor index — avoids O(n) array shift on every dequeue
  while (head < queue.length) {
    const current = queue[head++];
    const children = data.lineage[current] || [];
    for (const child of children) {
      if (!visited.has(child)) {
        visited.add(child);
        const [cStep, cIdx] = child.split(':').map(Number);
        if (cStep === viewingStep) {
          result.add(cIdx);
        } else if (cStep < viewingStep) {
          queue.push(child);
        }
      }
    }
  }
  return result;
}

function recomputeLineage() {
  lineageSets = selectedEdges.map(sel =>
    getDescendantsFromStep(activeRule, currentStep, sel.edgeIdx, sel.step)
  );
  const bar = document.getElementById('lineage-bar');
  const totalDesc = new Set();
  lineageSets.forEach(s => s.forEach(v => totalDesc.add(v)));
  document.getElementById('lineage-count').textContent =
    selectedEdges.length + ' selected, ' + totalDesc.size + ' descendant' + (totalDesc.size !== 1 ? 's' : '');
  bar.classList.add('active');
}

function clearLineage() {
  selectedEdges = [];
  lineageSets = [];
  document.getElementById('lineage-bar').classList.remove('active');
  renderSpatial();
}

// =========================================================================
// RULE CARDS
// =========================================================================
function renderRuleCards() {
  const container = document.getElementById('rule-cards');
  // §6.5 [M5] Use escHtml for all user-controlled fields to prevent XSS.
  // Use data-* attributes + addEventListener (no inline JS string quoting)
  // so IDs with any character content are safe.
  container.innerHTML = RULES.map(r => `
    <div class="rule-card ${r.id === activeRule ? 'active' : ''}" id="card-${escHtml(r.id)}" data-rule-id="${escHtml(r.id)}">
      ${r.isCustom ? `<button class="remove-custom" data-remove-id="${escHtml(r.id)}" title="Remove">&times;</button>` : ''}
      <div class="rule-name">${escHtml(r.name)}</div>
      <div class="rule-notation">${escHtml(r.notation)}</div>
      <div class="rule-desc">${escHtml(r.desc)}</div>
      <span class="rule-tag ${escHtml(r.tagClass)}">${escHtml(r.tag)}</span>
    </div>
  `).join('');
  container.querySelectorAll('.rule-card').forEach(card => {
    card.addEventListener('click', () => selectRule(card.dataset.ruleId));
  });
  container.querySelectorAll('.remove-custom').forEach(btn => {
    btn.addEventListener('click', e => {
      e.stopPropagation();
      removeCustomRule(btn.dataset.removeId);
    });
  });
}

function removeCustomRule(ruleId) {
  const idx = RULES.findIndex(r => r.id === ruleId);
  if (idx === -1) return;
  RULES.splice(idx, 1);
  delete DATA[ruleId];
  delete MULTIWAY[ruleId];
  if (activeRule === ruleId) selectRule(RULES[0].id);
  renderRuleCards();
}

function selectRule(ruleId) {
  // Stop playback before switching rules so the play button stays consistent
  if (playing) togglePlay();
  activeRule = ruleId;
  clearLineage();
  document.querySelectorAll('.rule-card').forEach(c => c.classList.remove('active'));
  const card = document.getElementById('card-' + ruleId);
  if (card) card.classList.add('active');

  // Clear previous error/stale so clicking the card always retries the fetch
  if (DATA[ruleId] && DATA[ruleId]._error) {
    delete DATA[ruleId];
    delete _computeState[ruleId];
    delete _computeStart[ruleId];
  }

  const data = DATA[ruleId];
  if (!data) {
    // Data not loaded yet — show overlay and clear any stale graph from prior rule
    updateExtendRow(); // hide extend button while loading
    showComputeOverlay('running', 'Server is computing…');
    startComputeTimer(ruleId);
    loadRuleData(ruleId);
    renderCurrentView(); // clears old SVG content so it doesn't bleed through overlay
    return;
  }

  // Cached data available — hide overlay
  stopComputeTimer();
  hideComputeOverlay();

  const maxStep = (data.states || []).length - 1;
  const slider = document.getElementById('step-slider');
  slider.max = maxStep;
  slider.value = Math.min(currentStep, maxStep);
  currentStep = +slider.value;

  // §6.8 [L5] Blurb comes from server /api/rules; custom rules store it in the rule object
  const rule = RULES.find(r => r.id === ruleId);
  const blurb = (rule && rule.blurb) || (data && data._blurb) || '';
  document.getElementById('theory-blurb').textContent = blurb;
  selectedMultiwayNode = null;
  selectedPath = null;

  // Load multiway in background
  if (!MULTIWAY[ruleId] && !ruleId.startsWith('custom_')) {
    loadMultiway(ruleId);
  }

  updateExtendRow();
  renderCurrentView();
}

// =========================================================================
// VIEWS
// =========================================================================
function setView(view, el) {
  currentView = view;
  document.querySelectorAll('.view-tab').forEach(t => t.classList.remove('active'));
  if (el) el.classList.add('active');

  document.getElementById('main-svg').style.display = view === 'spatial' ? '' : 'none';
  if (USE_CANVAS) document.getElementById('main-canvas').style.display = view === 'spatial' ? '' : 'none';
  document.getElementById('causal-view').className = 'causal-overlay' + (view === 'causal' ? ' active' : '');
  document.getElementById('multiway-view').className = 'causal-overlay' + (view === 'multiway' ? ' active' : '');
  document.getElementById('growth-view').style.display = view === 'growth' ? '' : 'none';
  if (view !== 'spatial') document.getElementById('lineage-bar').classList.remove('active');
  renderCurrentView();
}

function renderCurrentView() {
  if (currentView === 'spatial') { if (USE_CANVAS) renderSpatialCanvas(); else renderSpatial(); }
  else if (currentView === 'causal') renderCausal();
  else if (currentView === 'multiway') renderMultiway();
  else if (currentView === 'growth') renderGrowthAnalysis();
  updateStats();
}

function setStep(step) {
  currentStep = step;
  selectedMultiwayNode = null;
  selectedPath = null;
  document.getElementById('step-display').textContent = step;
  document.getElementById('step-slider').value = step;
  if (selectedEdges.length > 0) recomputeLineage();
  renderCurrentView();
}

// Debounced variant for the slider oninput event.
// Updates state and display immediately, but defers the expensive D3
// re-render to the next animation frame (capped at ~60 fps regardless
// of how many input events fire during a fast drag).
let _stepRafId = null;
function setStepFromSlider(step) {
  currentStep = step;
  selectedMultiwayNode = null;
  selectedPath = null;
  document.getElementById('step-display').textContent = step;
  if (selectedEdges.length > 0) recomputeLineage();
  if (_stepRafId !== null) cancelAnimationFrame(_stepRafId);
  _stepRafId = requestAnimationFrame(() => { _stepRafId = null; renderCurrentView(); });
}

// =========================================================================
// SPATIAL GRAPH
// =========================================================================
function renderSpatial() {
  const data = DATA[activeRule];
  if (!data || !data.states || currentStep >= data.states.length) {
    const svg = d3.select('#main-svg');
    svg.selectAll('*').remove();
    // Loading and error states are handled by #compute-overlay.
    // Only render SVG text for the valid-but-empty-step edge case.
    if (data && !data._error && data.states && currentStep >= data.states.length) {
      const rect = document.getElementById('canvas-area').getBoundingClientRect();
      svg.append('text').attr('x', rect.width / 2).attr('y', rect.height / 2)
        .attr('text-anchor', 'middle').attr('fill', isDark ? '#888' : '#666')
        .attr('font-size', 13).text('No data at this step');
    }
    return;
  }

  const mw = MULTIWAY[activeRule];
  const state = (selectedMultiwayNode && mw && mw.states && mw.states[selectedMultiwayNode])
    ? mw.states[selectedMultiwayNode].state
    : data.states[currentStep];

  const svg = d3.select('#main-svg');
  svg.selectAll('*').remove();
  svg.on('mousedown.nudge', null).on('mousemove.nudge', null).on('mouseup.nudge', null);
  const rect = document.getElementById('canvas-area').getBoundingClientRect();
  const width = rect.width, height = rect.height;

  const g = svg.append('g');
  const zoomBehavior = d3.zoom().scaleExtent([0.05, 20]).on('zoom', e => g.attr('transform', e.transform));
  svg.call(zoomBehavior);
  if (opts.nudge) svg.on('mousedown.zoom', null);

  svg.on('click', function(e) {
    if (e.target === this) clearLineage();
  });

  const nodeSet = new Set();
  const links = [];
  const selfLoops = [];
  const hyperedges = [];
  state.forEach((edge, idx) => {
    edge.forEach(n => nodeSet.add(n));
    hyperedges.push({ id: idx, nodes: edge });
    if (edge.length === 2 && edge[0] === edge[1]) {
      selfLoops.push({ node: edge[0], edgeIdx: idx });
    } else if (edge.length === 2) {
      links.push({ source: edge[0], target: edge[1], edgeIdx: idx });
    } else {
      for (let i = 0; i < edge.length - 1; i++) {
        links.push({ source: edge[i], target: edge[i+1], edgeIdx: idx });
      }
    }
  });

  // Curve offsets for parallel edges
  const pairCount = {};
  links.forEach(l => {
    const key = l.source + '-' + l.target;
    pairCount[key] = (pairCount[key] || 0) + 1;
    l._pairKey = key;
    l._pairIdx = pairCount[key] - 1;
  });
  links.forEach(l => {
    const total = pairCount[l._pairKey];
    if (total <= 1) { l._curve = 0; }
    else {
      const i = l._pairIdx;
      const sign = (i % 2 === 0) ? 1 : -1;
      l._curve = sign * Math.ceil((i + 1) / 2) * 18;
    }
  });
  const nodes = Array.from(nodeSet).map(id => {
    const prev = _prevNodePositions.get(id);
    return prev ? { id, x: prev.x, y: prev.y } : { id };
  });
  const nodeById = new Map(nodes.map(n => [n.id, n]));

  const nodeR = Math.max(0.5, Math.min(2, 60 / Math.sqrt(nodes.length)));
  const baseEdgeWidth = Math.max(0.8, 2.5 - nodes.length / 400);

  // Birth step colors from server data
  const birthSteps = (data.birthSteps && data.birthSteps[currentStep]) || [];
  const maxBirthStep = Math.max(1, currentStep);
  function edgeBirthColor(edgeIdx) {
    const birth = birthSteps[edgeIdx] !== undefined ? birthSteps[edgeIdx] : 0;
    return getPalette()[birth % getPalette().length];
  }

  const hullG = g.append('g');

  // Links
  const link = g.append('g').selectAll('path').data(links).join('path')
    .attr('fill', 'none')
    .attr('stroke', d => {
      if (selectedEdges.length > 0) {
        const selColor = getEdgeSelColor(d.edgeIdx);
        if (selColor) return selColor;
      }
      return opts.colors ? edgeBirthColor(d.edgeIdx) : (isDark ? '#3a3a5e' : '#8888aa');
    })
    .attr('stroke-width', d => {
      if (selectedEdges.length > 0 && getEdgeSelColor(d.edgeIdx)) return baseEdgeWidth * 2;
      return baseEdgeWidth;
    })
    .attr('stroke-opacity', d => {
      if (selectedEdges.length > 0) {
        return getEdgeSelColor(d.edgeIdx) ? 1 : 0.25;
      }
      return 0.65;
    })
    .style('cursor', 'pointer')
    .on('click', function(e, d) {
      e.stopPropagation();
      const existIdx = selectedEdges.findIndex(s => s.step === currentStep && s.edgeIdx === d.edgeIdx);
      if (existIdx >= 0) {
        selectedEdges.splice(existIdx, 1);
        lineageSets.splice(existIdx, 1);
      } else {
        if (selectedEdges.length >= 3) {
          selectedEdges.shift();
          lineageSets.shift();
        }
        selectedEdges.push({ edgeIdx: d.edgeIdx, step: currentStep });
      }
      if (selectedEdges.length === 0) { clearLineage(); return; }
      recomputeLineage();
      renderSpatial();
    });

  // Node birth colors
  const nodeBirthMap = {};
  state.forEach((edge, idx) => {
    const birth = birthSteps[idx] !== undefined ? birthSteps[idx] : 0;
    edge.forEach(n => {
      if (nodeBirthMap[n] === undefined || birth < nodeBirthMap[n]) nodeBirthMap[n] = birth;
    });
  });
  function nodeBirthColor(nodeId) {
    const birth = nodeBirthMap[nodeId] !== undefined ? nodeBirthMap[nodeId] : 0;
    return getPalette()[birth % getPalette().length];
  }

  const loopG = g.append('g');

  // Precompute self-loop stacking indices; create path elements once (updated by tick handler)
  const loopIdxByNode = {};
  selfLoops.forEach(sl => {
    sl._loopIdx = loopIdxByNode[sl.node] || 0;
    loopIdxByNode[sl.node] = sl._loopIdx + 1;
  });
  const loopPaths = loopG.selectAll('path').data(selfLoops).join('path')
    .attr('fill', 'none')
    .attr('stroke', sl => {
      if (selectedEdges.length > 0) {
        const c = getEdgeSelColor(sl.edgeIdx);
        if (c) return c;
      }
      return opts.colors ? edgeBirthColor(sl.edgeIdx) : (isDark ? '#3a3a5e' : '#8888aa');
    })
    .attr('stroke-width', sl => selectedEdges.length > 0 && getEdgeSelColor(sl.edgeIdx) ? baseEdgeWidth * 2 : baseEdgeWidth)
    .attr('stroke-opacity', sl => selectedEdges.length > 0 ? (getEdgeSelColor(sl.edgeIdx) ? 1 : 0.25) : 0.65);

  // Nodes
  const node = g.append('g').selectAll('circle').data(nodes).join('circle')
    .attr('r', nodeR)
    .attr('fill', (d, i) => {
      if (selectedEdges.length > 0) {
        let bestColor = null;
        for (const l of links) {
          const nid = typeof l.source === 'object' ? l.source.id : l.source;
          const tid = typeof l.target === 'object' ? l.target.id : l.target;
          if (nid === d.id || tid === d.id) {
            const c = getEdgeSelColor(l.edgeIdx);
            if (c) { bestColor = c; break; }
          }
        }
        if (bestColor) return bestColor;
        return isDark ? '#222' : '#ccc';
      }
      return opts.colors ? nodeBirthColor(d.id) : getPalette()[i % getPalette().length];
    })
    .attr('stroke', () => isDark ? '#08080c' : '#ffffff')
    .attr('stroke-width', 0.3)
    .attr('opacity', d => {
      if (selectedEdges.length > 0) {
        for (const l of links) {
          const nid = typeof l.source === 'object' ? l.source.id : l.source;
          const tid = typeof l.target === 'object' ? l.target.id : l.target;
          if ((nid === d.id || tid === d.id) && getEdgeSelColor(l.edgeIdx)) return 1;
        }
        return 0.2;
      }
      return 1;
    })
    .style('cursor', 'pointer')
    .on('mouseenter', (ev, d) => showTooltip(ev, `Node ${d.id}`))
    .on('mouseleave', hideTooltip)
    .call(d3.drag()
      .on('start', (e,d) => { if (!e.active) simulation.alphaTarget(0.3).restart(); d.fx=d.x; d.fy=d.y; })
      .on('drag', (e,d) => { d.fx=e.x; d.fy=e.y; })
      .on('end', (e,d) => { if (!e.active) simulation.alphaTarget(0); d.fx=null; d.fy=null; })
    );

  // Labels — cache the D3 selection so the tick handler can call .attr() directly
  const labelG = g.append('g');
  let labelSel = null;
  if (opts.labels) {
    labelSel = labelG.selectAll('text').data(nodes).join('text')
      .attr('font-size', Math.max(6, 8 - nodes.length/100))
      .attr('font-family', 'JetBrains Mono, monospace')
      .attr('fill', isDark ? '#888' : '#666')
      .attr('dx', nodeR + 2).attr('dy', 3)
      .text(d => d.id);
  }

  if (simulation) {
    // Save current positions for warmstart on the next renderSpatial call
    simulation.nodes().forEach(n => {
      if (n.x != null) _prevNodePositions.set(n.id, { x: n.x, y: n.y });
    });
    simulation.stop();
  }
  const baseDist = 20 + 200/Math.sqrt(nodes.length);
  simulation = d3.forceSimulation(nodes)
    .force('link', d3.forceLink(links).id(d => d.id)
      .distance(d => ((selectedEdges.length > 0 && getEdgeSelColor(d.edgeIdx)) ? baseDist * 0.2 : baseDist) * forceParams.linkDistMult)
      .strength(d => (selectedEdges.length > 0 && getEdgeSelColor(d.edgeIdx)) ? 1.0 : forceParams.linkStrength))
    .force('charge', d3.forceManyBody().strength((-30 - 2000/nodes.length) * forceParams.chargeMult).distanceMax(300))
    .force('center', d3.forceCenter(width/2, height/2))
    .force('collision', d3.forceCollide(nodeR + 0.5))
    .on('tick', () => {
      link.attr('d', d => {
        const sx = d.source.x, sy = d.source.y, tx = d.target.x, ty = d.target.y;
        if (d._curve === 0) return 'M' + sx + ',' + sy + 'L' + tx + ',' + ty;
        const mx = (sx + tx) / 2, my = (sy + ty) / 2;
        const dx = tx - sx, dy = ty - sy;
        const len = Math.sqrt(dx*dx + dy*dy) || 1;
        const nx = -dy / len, ny = dx / len;
        const cx = mx + nx * d._curve, cy = my + ny * d._curve;
        return 'M' + sx + ',' + sy + 'Q' + cx + ',' + cy + ' ' + tx + ',' + ty;
      });
      node.attr('cx',d=>d.x).attr('cy',d=>d.y);
      if (labelSel) labelSel.attr('x',d=>d.x).attr('y',d=>d.y);
      if (opts.hulls) drawHulls(hullG, hyperedges, nodeById, edgeBirthColor);
      // Self-loops — path elements already exist; just update `d` in place
      loopPaths.attr('d', sl => {
        const n = nodeById.get(sl.node);
        if (!n || n.x == null) return '';
        const r = nodeR * 3 + sl._loopIdx * nodeR * 2.5;
        return `M${n.x},${n.y - nodeR} A${r},${r} 0 1,1 ${n.x + 0.01},${n.y - nodeR}`;
      });
    });

  // Nudge mode
  if (opts.nudge) {
    svg.style('cursor', 'crosshair');
    const nudgeRadius = 140, nudgeStrength = 2.0;
    let nudging = false;
    svg.on('mousedown.nudge', function(e) {
      if (e.target.tagName === 'circle' || e.target.tagName === 'line') return;
      nudging = true;
    });
    svg.on('mousemove.nudge', function(e) {
      if (!nudging) return;
      const transform = d3.zoomTransform(svg.node());
      const mx = (e.offsetX - transform.x) / transform.k;
      const my = (e.offsetY - transform.y) / transform.k;
      nodes.forEach(n => {
        const dx = n.x - mx, dy = n.y - my;
        const dist = Math.sqrt(dx*dx + dy*dy);
        if (dist < nudgeRadius && dist > 1) {
          const force = nudgeStrength * (1 - dist / nudgeRadius);
          n.vx += (dx / dist) * force * 10;
          n.vy += (dy / dist) * force * 10;
        }
      });
      simulation.alpha(0.3).restart();
    });
    svg.on('mouseup.nudge', () => { nudging = false; });
  }
}

function drawHulls(hullG, hyperedges, nodeById, birthColorFn) {
  // Data-join preserves path elements across ticks; only the `d` attribute is recomputed
  const hEdges = hyperedges.filter(h => h.nodes.length > 2);
  hullG.selectAll('path')
    .data(hEdges, h => h.id)
    .join(
      enter => enter.append('path')
        .attr('fill-opacity', 0.06)
        .attr('stroke-opacity', 0.15)
        .attr('stroke-width', 1)
        .attr('fill', h => birthColorFn ? birthColorFn(h.id) : getPalette()[0])
        .attr('stroke', h => birthColorFn ? birthColorFn(h.id) : getPalette()[0])
    )
    .attr('d', h => {
      const pts = h.nodes.map(nid => {
        const n = nodeById.get(nid);
        return n && n.x != null ? [n.x, n.y] : null;
      }).filter(Boolean);
      if (pts.length < 3) return '';
      const hull = d3.polygonHull(pts);
      return hull ? 'M' + hull.join('L') + 'Z' : '';
    });
}

// =========================================================================
// CANVAS RENDERER PATH  (?renderer=canvas)
// Phase 2: main-thread d3 simulation, canvas drawing, SVG hit-test overlay.
// Phase 3 will replace the simulation with layout-worker.js postMessage ticks.
// =========================================================================
function renderSpatialCanvas() {
  const data = DATA[activeRule];
  if (!data || !data.states || currentStep >= data.states.length) {
    // Clear canvas + overlay on empty/error state
    const canvasEl = document.getElementById('main-canvas');
    if (canvasEl) {
      const ctx = canvasEl.getContext('2d');
      ctx.clearRect(0, 0, canvasEl.width, canvasEl.height);
    }
    d3.select('#main-svg').selectAll('*').remove();
    return;
  }

  const mw = MULTIWAY[activeRule];
  const state = (selectedMultiwayNode && mw && mw.states && mw.states[selectedMultiwayNode])
    ? mw.states[selectedMultiwayNode].state
    : data.states[currentStep];

  // ── Build graph topology (same as renderSpatial) ─────────────────────────
  const nodeSet  = new Set();
  const links    = [];
  const selfLoops = [];
  const hyperedges = [];
  state.forEach((edge, idx) => {
    edge.forEach(n => nodeSet.add(n));
    hyperedges.push({ id: idx, nodes: edge });
    if (edge.length === 2 && edge[0] === edge[1]) {
      selfLoops.push({ node: edge[0], edgeIdx: idx });
    } else if (edge.length === 2) {
      links.push({ source: edge[0], target: edge[1], edgeIdx: idx });
    } else {
      for (let i = 0; i < edge.length - 1; i++) {
        links.push({ source: edge[i], target: edge[i + 1], edgeIdx: idx });
      }
    }
  });

  // Curve offsets for parallel edges
  const pairCount = {};
  links.forEach(l => {
    const key = l.source + '-' + l.target;
    pairCount[key] = (pairCount[key] || 0) + 1;
    l._pairKey = key; l._pairIdx = pairCount[key] - 1;
  });
  links.forEach(l => {
    const total = pairCount[l._pairKey];
    if (total <= 1) { l._curve = 0; }
    else {
      const i = l._pairIdx;
      const sign = (i % 2 === 0) ? 1 : -1;
      l._curve = sign * Math.ceil((i + 1) / 2) * 18;
    }
  });

  // Warmstart node positions
  const nodes = Array.from(nodeSet).map(id => {
    const prev = _prevNodePositions.get(id);
    return prev ? { id, x: prev.x, y: prev.y } : { id };
  });
  const nodeById = new Map(nodes.map(n => [n.id, n]));

  const nodeR = Math.max(0.5, Math.min(2, 60 / Math.sqrt(nodes.length)));
  const baseEdgeWidth = Math.max(0.8, 2.5 - nodes.length / 400);

  // ── Birth-step colour helpers ─────────────────────────────────────────────
  const birthSteps = (data.birthSteps && data.birthSteps[currentStep]) || [];
  function edgeBirthColor(edgeIdx) {
    const birth = birthSteps[edgeIdx] !== undefined ? birthSteps[edgeIdx] : 0;
    return getPalette()[birth % getPalette().length];
  }
  const nodeBirthMap = {};
  state.forEach((edge, idx) => {
    const birth = birthSteps[idx] !== undefined ? birthSteps[idx] : 0;
    edge.forEach(n => {
      if (nodeBirthMap[n] === undefined || birth < nodeBirthMap[n]) nodeBirthMap[n] = birth;
    });
  });
  function nodeBirthColor(nodeId) {
    const birth = nodeBirthMap[nodeId] !== undefined ? nodeBirthMap[nodeId] : 0;
    return getPalette()[birth % getPalette().length];
  }

  // Precompute base colours onto data objects (canvas renderer reads these)
  const palette = getPalette();
  nodes.forEach((n, i) => {
    n._fill = opts.colors ? nodeBirthColor(n.id) : palette[i % palette.length];
  });
  links.forEach(l => {
    l._stroke = opts.colors ? edgeBirthColor(l.edgeIdx) : (isDark ? '#3a3a5e' : '#8888aa');
  });
  selfLoops.forEach(sl => {
    sl._stroke = opts.colors ? edgeBirthColor(sl.edgeIdx) : (isDark ? '#3a3a5e' : '#8888aa');
  });
  hyperedges.forEach(h => { h._color = edgeBirthColor(h.id); });

  // Self-loop stacking indices
  const loopIdxByNode = {};
  selfLoops.forEach(sl => {
    sl._loopIdx = loopIdxByNode[sl.node] || 0;
    loopIdxByNode[sl.node] = sl._loopIdx + 1;
  });

  // ── Canvas setup ──────────────────────────────────────────────────────────
  const canvasEl = document.getElementById('main-canvas');
  const rect = document.getElementById('canvas-area').getBoundingClientRect();
  const width = rect.width, height = rect.height;
  canvasEl.style.display = '';
  CanvasRenderer.init(canvasEl);
  CanvasRenderer.resize(width, height);

  // ── SVG as hit-test overlay (no visual paint, transparent shapes only) ────
  const svg = d3.select('#main-svg');
  svg.selectAll('*').remove();
  svg.on('mousedown.nudge', null).on('mousemove.nudge', null).on('mouseup.nudge', null);
  svg.on('click', function(e) { if (e.target === this) clearLineage(); });

  const overlayG     = svg.append('g');
  const overlayEdgeG = overlayG.append('g');  // edges below nodes (z-order)
  const overlayNodeG = overlayG.append('g');

  // Helper: compute edge path string for overlay (same formula as renderSpatial tick)
  function _overlayEdgePath(d) {
    const sx = d.source.x, sy = d.source.y, tx = d.target.x, ty = d.target.y;
    if (!sx && sx !== 0) return '';
    if (d._curve === 0) return `M${sx},${sy}L${tx},${ty}`;
    const mx = (sx + tx) / 2, my = (sy + ty) / 2;
    const dx = tx - sx, dy = ty - sy;
    const len = Math.sqrt(dx * dx + dy * dy) || 1;
    const nx = -dy / len, ny = dx / len;
    const cx = mx + nx * d._curve, cy = my + ny * d._curve;
    return `M${sx},${sy}Q${cx},${cy} ${tx},${ty}`;
  }

  // ── Zoom behaviour ────────────────────────────────────────────────────────
  const zoomBehavior = d3.zoom().scaleExtent([0.05, 20]).on('zoom', e => {
    overlayG.attr('transform', e.transform);
    CanvasRenderer.setTransform(e.transform);
    CanvasRenderer.drawFrame({
      nodes, nodeById, links, selfLoops, hyperedges,
      nodeR, baseEdgeWidth, isDark, opts, selectedEdges, getEdgeSelColor,
    });
  });
  svg.call(zoomBehavior);

  // ── Force simulation (main-thread, Phase 2) ───────────────────────────────
  if (simulation) {
    simulation.nodes().forEach(n => {
      if (n.x != null) _prevNodePositions.set(n.id, { x: n.x, y: n.y });
    });
    simulation.stop();
  }
  const baseDist = 20 + 200 / Math.sqrt(nodes.length);
  simulation = d3.forceSimulation(nodes)
    .force('link', d3.forceLink(links).id(d => d.id)
      .distance(d => ((selectedEdges.length > 0 && getEdgeSelColor(d.edgeIdx)) ? baseDist * 0.2 : baseDist) * forceParams.linkDistMult)
      .strength(d => (selectedEdges.length > 0 && getEdgeSelColor(d.edgeIdx)) ? 1.0 : forceParams.linkStrength))
    .force('charge', d3.forceManyBody().strength((-30 - 2000 / nodes.length) * forceParams.chargeMult).distanceMax(300))
    .force('center', d3.forceCenter(width / 2, height / 2))
    .force('collision', d3.forceCollide(nodeR + 0.5))
    .on('tick', () => {
      // ── Draw canvas frame ─────────────────────────────────────────────────
      CanvasRenderer.drawFrame({
        nodes, nodeById, links, selfLoops, hyperedges,
        nodeR, baseEdgeWidth, isDark, opts, selectedEdges, getEdgeSelColor,
      });

      // ── Rebuild SVG overlay (transparent hit-test geometry) ───────────────
      // Edge overlay paths (for lineage clicking)
      overlayEdgeG.selectAll('path')
        .data(links, d => d.edgeIdx)
        .join(
          enter => enter.append('path')
            .attr('stroke', 'transparent')
            .attr('stroke-width', Math.max(6, baseEdgeWidth + 4))
            .attr('fill', 'none')
            .style('cursor', 'pointer')
            .on('click', function(e, d) {
              e.stopPropagation();
              const existIdx = selectedEdges.findIndex(s => s.step === currentStep && s.edgeIdx === d.edgeIdx);
              if (existIdx >= 0) {
                selectedEdges.splice(existIdx, 1);
                lineageSets.splice(existIdx, 1);
              } else {
                if (selectedEdges.length >= 3) { selectedEdges.shift(); lineageSets.shift(); }
                selectedEdges.push({ edgeIdx: d.edgeIdx, step: currentStep });
              }
              if (selectedEdges.length === 0) { clearLineage(); return; }
              recomputeLineage();
              renderSpatialCanvas();
            })
        )
        .attr('d', _overlayEdgePath);

      // Node overlay circles (for hover / drag)
      overlayNodeG.selectAll('circle')
        .data(nodes, d => d.id)
        .join(
          enter => enter.append('circle')
            .attr('r', nodeR + 2)
            .attr('fill', 'transparent')
            .style('cursor', 'pointer')
            .on('mouseenter', (ev, d) => showTooltip(ev, `Node ${d.id}`))
            .on('mouseleave', hideTooltip)
            .call(d3.drag()
              .on('start', (e, d) => { if (!e.active) simulation.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y; })
              .on('drag',  (e, d) => { d.fx = e.x; d.fy = e.y; })
              .on('end',   (e, d) => { if (!e.active) simulation.alphaTarget(0); d.fx = null; d.fy = null; })
            )
        )
        .attr('cx', d => d.x)
        .attr('cy', d => d.y);
    });
}

// =========================================================================
// CAUSAL GRAPH
// =========================================================================
function renderCausal() {
  const data = DATA[activeRule];
  if (!data || !data.causal_edges) return;

  const svg = d3.select('#causal-svg');
  svg.selectAll('*').remove();
  const rect = document.getElementById('canvas-area').getBoundingClientRect();
  const width = rect.width, height = rect.height;
  svg.attr('width', width).attr('height', height);

  let allEvNodes = [];
  let allCausalEdges = [];
  let eventInfo = {};
  // produced-edge lookup: JSON(edge) → [eventId, ...] — replaces the O(n²)
  // inner scan when computing causal edges for alternative matches.
  const producedEdgeIndex = new Map();

  function _indexProduced(evId, produced) {
    for (const e of (produced || [])) {
      const k = JSON.stringify(e);
      if (!producedEdgeIndex.has(k)) producedEdgeIndex.set(k, []);
      producedEdgeIndex.get(k).push(evId);
    }
  }

  let greedyStep = 0;
  for (const stepEvents of (data.events || [])) {
    for (const ev of stepEvents) {
      allEvNodes.push({ id: ev.id, step: greedyStep, consumed: ev.consumed, produced: ev.produced, onPath: true });
      eventInfo[ev.id] = ev;
      _indexProduced(ev.id, ev.produced);
    }
    greedyStep++;
  }
  for (const [a, b] of (data.causal_edges || [])) {
    allCausalEdges.push({ source: a, target: b });
  }

  // Alternative matches using lightweight client-side matching
  const ruleDef = RULES.find(r => r.id === activeRule);
  let parsedRule = null;
  if (ruleDef) parsedRule = parseNotation(ruleDef.notation);
  else if (data._customParsed) parsedRule = data._customParsed;

  if (parsedRule && data.states) {
    let altId = 100000;
    const maxAltStep = Math.min((data.states || []).length - 1, currentStep);

    for (let s = 0; s < maxAltStep; s++) {
      const state = data.states[s];
      if (!state) continue;
      const matches = HGEngine.findMatches(state, parsedRule.lhs);

      const greedyConsumedSigs = new Set();
      const stepEvts = (data.events[s] || []);
      for (const ev of stepEvts) {
        greedyConsumedSigs.add(JSON.stringify(ev.consumed));
      }

      for (const [matchedIndices, binding] of matches) {
        const consumed = matchedIndices.map(i => state[i]);
        const sig = JSON.stringify(consumed);
        if (greedyConsumedSigs.has(sig)) continue;

        const node = { id: altId, step: s, consumed, produced: [], onPath: false };
        allEvNodes.push(node);
        eventInfo[altId] = node;

        // O(consumed_edges) lookup instead of O(all_events) scan
        const seen = new Set();
        for (const ce of consumed) {
          for (const prevId of (producedEdgeIndex.get(JSON.stringify(ce)) || [])) {
            if (!seen.has(prevId) && eventInfo[prevId] && eventInfo[prevId].step < s) {
              seen.add(prevId);
              allCausalEdges.push({ source: prevId, target: altId });
            }
          }
        }
        altId++;
      }
    }
  }

  const visibleEvents = allEvNodes.filter(e => e.step < currentStep);
  const visibleIds = new Set(visibleEvents.map(e => e.id));
  const pathEventIds = new Set(visibleEvents.filter(e => e.onPath).map(e => e.id));
  const filteredEdges = allCausalEdges.filter(e => visibleIds.has(e.source) && visibleIds.has(e.target));

  const eventStep = {};
  for (const ev of allEvNodes) eventStep[ev.id] = ev.step;
  const maxCausalStep = Math.max(1, ...visibleEvents.map(e => e.step));

  const nodes = visibleEvents.map(ev => ({
    id: ev.id,
    x: width / 2 + (Math.random() - 0.5) * 40,
    y: 50 + ev.step * (height - 100) / maxCausalStep
  }));

  if (nodes.length === 0) {
    svg.append('text').attr('x', width/2).attr('y', height/2)
      .attr('text-anchor', 'middle').attr('fill', isDark ? '#666' : '#999')
      .attr('font-size', 14).text('No causal data at this step');
    return;
  }

  const g = svg.append('g');
  svg.call(d3.zoom().scaleExtent([0.05, 20]).on('zoom', e => g.attr('transform', e.transform)));

  const nodeR = Math.max(1.5, 4 - Math.log10(nodes.length + 1) * 1.2);

  g.append('defs').append('marker')
    .attr('id', 'arrow').attr('viewBox', '0 -3 6 6').attr('refX', 8)
    .attr('markerWidth', 5).attr('markerHeight', 5).attr('orient', 'auto')
    .append('path').attr('d', 'M0,-3L6,0L0,3').attr('fill', isDark ? '#4cdd8a60' : '#1a9f5c80');

  g.append('defs').append('marker')
    .attr('id', 'arrow-red').attr('viewBox', '0 -3 6 6').attr('refX', 8)
    .attr('markerWidth', 5).attr('markerHeight', 5).attr('orient', 'auto')
    .append('path').attr('d', 'M0,-3L6,0L0,3').attr('fill', '#ff4444');

  const link = g.append('g').selectAll('line').data(filteredEdges).join('line')
    .attr('stroke', d => {
      const s = typeof d.source === 'object' ? d.source.id : d.source;
      const t = typeof d.target === 'object' ? d.target.id : d.target;
      return (pathEventIds.has(s) && pathEventIds.has(t)) ? '#ff444480' : (isDark ? '#4cdd8a40' : '#1a9f5c50');
    })
    .attr('stroke-width', d => {
      const s = typeof d.source === 'object' ? d.source.id : d.source;
      const t = typeof d.target === 'object' ? d.target.id : d.target;
      return (pathEventIds.has(s) && pathEventIds.has(t)) ? 2 : 0.8;
    })
    .attr('marker-end', d => {
      const s = typeof d.source === 'object' ? d.source.id : d.source;
      const t = typeof d.target === 'object' ? d.target.id : d.target;
      return (pathEventIds.has(s) && pathEventIds.has(t)) ? 'url(#arrow-red)' : 'url(#arrow)';
    });

  const node_el = g.append('g').selectAll('circle').data(nodes).join('circle')
    .attr('r', d => pathEventIds.has(d.id) ? nodeR * 1.5 : nodeR)
    .attr('fill', d => pathEventIds.has(d.id) ? '#ff4444' : (isDark ? '#4cdd8a' : '#1a9f5c'))
    .attr('fill-opacity', d => pathEventIds.has(d.id) ? 1 : 0.5)
    .attr('stroke', d => pathEventIds.has(d.id) ? '#ff444480' : (isDark ? '#08080c' : '#fff'))
    .attr('stroke-width', d => pathEventIds.has(d.id) ? 2 : 0.5)
    .style('cursor', 'pointer')
    .on('mouseenter', (ev, d) => {
      const info = eventInfo[d.id];
      if (!info) { showTooltip(ev, `Event ${d.id} (step ${eventStep[d.id] || '?'})`); return; }
      const consumed = (info.consumed || []).map(e => '{' + e.join(',') + '}').join(' ');
      const produced = (info.produced || []).map(e => '{' + e.join(',') + '}').join(' ');
      const onPath = pathEventIds.has(d.id) ? ' \u2714 on displayed path' : ' (alternative branch)';
      showTooltip(ev,
        `Event #${d.id}  (step ${eventStep[d.id] || '?'})${onPath}\n` +
        `Consumed: ${consumed || 'none'}\n` +
        `Produced: ${produced || 'none'}`
      );
    })
    .on('mouseleave', hideTooltip);

  d3.forceSimulation(nodes)
    .force('link', d3.forceLink(filteredEdges).id(d => d.id).distance(40).strength(0.15))
    .force('charge', d3.forceManyBody().strength(-120).distanceMax(500))
    .force('y', d3.forceY(d => 50 + (eventStep[d.id] || 0) * (height - 100) / maxCausalStep).strength(0.6))
    .force('x', d3.forceX(width / 2).strength(0.03))
    .on('tick', () => {
      link.attr('x1',d=>d.source.x).attr('y1',d=>d.source.y)
          .attr('x2',d=>d.target.x).attr('y2',d=>d.target.y);
      node_el.attr('cx',d=>d.x).attr('cy',d=>d.y);
    });
}

// =========================================================================
// GROWTH ANALYSIS
// =========================================================================
function renderGrowthAnalysis() {
  const container = document.getElementById('growth-view');
  container.style.background = 'var(--bg)';
  const data = DATA[activeRule];
  if (!data) return;

  const stats = data.stats || [];
  const rule = RULES.find(r => r.id === activeRule);

  // §6.5 [M5] Rule name/notation escaped; stats are numeric (safe)
  container.innerHTML = `
    <div style="max-width: 800px; margin: 0 auto;">
      <h2 style="font-size: 18px; font-weight: 600; color: var(--text-heading); margin-bottom: 4px;">${escHtml(rule ? rule.name : activeRule)}</h2>
      <p style="font-family: JetBrains Mono, monospace; font-size: 12px; color: var(--accent); margin-bottom: 24px;">${escHtml(rule ? rule.notation : '')}</p>
      <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 24px;">
        <div style="background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 16px;">
          <h4 style="font-size: 10px; text-transform: uppercase; letter-spacing: 1px; color: var(--text-dim); margin-bottom: 12px;">Node &amp; Edge Growth</h4>
          <canvas id="chart-growth" style="width:100%; height:180px; display:block;"></canvas>
        </div>
        <div style="background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 16px;">
          <h4 style="font-size: 10px; text-transform: uppercase; letter-spacing: 1px; color: var(--text-dim); margin-bottom: 12px;">Dimension Estimate</h4>
          <canvas id="chart-dim" style="width:100%; height:180px; display:block;"></canvas>
        </div>
      </div>
      <div style="background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 16px;">
        <h4 style="font-size: 10px; text-transform: uppercase; letter-spacing: 1px; color: var(--text-dim); margin-bottom: 12px;">Step-by-Step Data</h4>
        <table style="width: 100%; font-size: 12px; border-collapse: collapse;">
          <thead><tr style="border-bottom: 1px solid var(--border);">
            <th style="text-align: left; padding: 6px; color: var(--text-dim);">Step</th>
            <th style="text-align: right; padding: 6px; color: var(--text-dim);">Nodes</th>
            <th style="text-align: right; padding: 6px; color: var(--text-dim);">Edges</th>
            <th style="text-align: right; padding: 6px; color: var(--text-dim);">Dimension</th>
            <th style="text-align: right; padding: 6px; color: var(--text-dim);">Growth</th>
          </tr></thead>
          <tbody>
            ${stats.filter(s => s.num_nodes !== undefined).map((s, i, arr) => {
              const growth = i > 0 ? ((s.num_nodes / arr[i-1].num_nodes - 1) * 100).toFixed(1) + '%' : '--';
              return `<tr style="border-bottom: 1px solid var(--border);">
                <td style="padding: 6px; font-family: JetBrains Mono, monospace;">${s.step}</td>
                <td style="padding: 6px; text-align: right; font-family: JetBrains Mono, monospace; color: var(--accent);">${s.num_nodes}</td>
                <td style="padding: 6px; text-align: right; font-family: JetBrains Mono, monospace; color: var(--pink);">${s.num_edges}</td>
                <td style="padding: 6px; text-align: right; font-family: JetBrains Mono, monospace; color: var(--green);">${s.estimated_dimension != null ? s.estimated_dimension.toFixed(3) : '--'}</td>
                <td style="padding: 6px; text-align: right; font-family: JetBrains Mono, monospace; color: var(--orange);">${growth}</td>
              </tr>`;
            }).join('')}
          </tbody>
        </table>
      </div>
    </div>
  `;

  const growthStats = stats.filter(s => s.num_nodes !== undefined);
  const dimStats = stats.filter(s => s.estimated_dimension != null);

  function redrawCharts() {
    drawLineChart('chart-growth', growthStats, [
      { key: 'num_nodes', color: '#6c7bff', label: 'Nodes' },
      { key: 'num_edges', color: '#ff6b9d', label: 'Edges' }
    ]);
    if (dimStats.length > 0) {
      drawLineChart('chart-dim', dimStats, [
        { key: 'estimated_dimension', color: '#4cdd8a', label: 'Dimension' }
      ], true);
    }
  }
  redrawCharts();

  // §6.7 [L3/L4] ResizeObserver: re-draw charts when container resizes
  if (_growthResizeObs) _growthResizeObs.disconnect();
  _growthResizeObs = new ResizeObserver(() => {
    if (currentView === 'growth') redrawCharts();
  });
  _growthResizeObs.observe(container);
}

// §6.7 [L3/L4] ResizeObserver instance for growth-view charts
let _growthResizeObs = null;

function drawLineChart(canvasId, data, series, fixedRange) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;

  // §6.7 [L3/L4] HiDPI: scale canvas by devicePixelRatio for crisp rendering
  const dpr = window.devicePixelRatio || 1;
  const logicalW = canvas.offsetWidth || 340;
  const logicalH = canvas.offsetHeight || 180;
  canvas.width = Math.round(logicalW * dpr);
  canvas.height = Math.round(logicalH * dpr);
  canvas.style.width = logicalW + 'px';
  canvas.style.height = logicalH + 'px';

  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);
  const W = logicalW, H = logicalH;
  const pad = { top: 20, right: 16, bottom: 24, left: 48 };
  const cw = W - pad.left - pad.right;
  const ch = H - pad.top - pad.bottom;

  const bgColor = getComputedStyle(document.documentElement).getPropertyValue('--chart-bg').trim();
  const gridColor = getComputedStyle(document.documentElement).getPropertyValue('--chart-grid').trim();
  const dimColor = getComputedStyle(document.documentElement).getPropertyValue('--text-dim').trim();

  ctx.fillStyle = bgColor;
  ctx.fillRect(0, 0, W, H);

  if (data.length < 2) return;

  const xMin = data[0].step;
  const xMax = data[data.length-1].step;
  const xScale = v => pad.left + (v - xMin) / (xMax - xMin) * cw;

  ctx.strokeStyle = gridColor;
  ctx.lineWidth = 0.5;
  for (let i = 0; i <= 4; i++) {
    const y = pad.top + ch * i / 4;
    ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(W - pad.right, y); ctx.stroke();
  }

  series.forEach(s => {
    const vals = data.map(d => d[s.key]).filter(v => v != null);
    if (vals.length === 0) return;
    let yMin = fixedRange ? 0 : Math.min(...vals) * 0.9;
    let yMax = fixedRange ? Math.max(3.5, Math.max(...vals) * 1.1) : Math.max(...vals) * 1.1;
    if (yMax === yMin) yMax = yMin + 1;
    const yScale = v => pad.top + ch - (v - yMin) / (yMax - yMin) * ch;

    ctx.strokeStyle = s.color; ctx.lineWidth = 2;
    ctx.beginPath();
    let first = true;
    data.forEach(d => {
      if (d[s.key] == null) return;
      const x = xScale(d.step), y = yScale(d[s.key]);
      if (first) { ctx.moveTo(x, y); first = false; } else ctx.lineTo(x, y);
    });
    ctx.stroke();

    data.forEach(d => {
      if (d[s.key] == null) return;
      ctx.beginPath();
      ctx.arc(xScale(d.step), yScale(d[s.key]), 3, 0, Math.PI * 2);
      ctx.fillStyle = s.color; ctx.fill();
    });

    ctx.fillStyle = dimColor;
    ctx.font = '9px JetBrains Mono'; ctx.textAlign = 'right';
    for (let i = 0; i <= 4; i++) {
      const v = yMin + (yMax - yMin) * (1 - i/4);
      ctx.fillText(v >= 10 ? Math.round(v) : v.toFixed(1), pad.left - 6, pad.top + ch * i / 4 + 3);
    }
  });

  ctx.fillStyle = dimColor;
  ctx.font = '9px JetBrains Mono'; ctx.textAlign = 'center';
  data.forEach(d => { ctx.fillText(d.step, xScale(d.step), H - 4); });

  let lx = pad.left + 4;
  series.forEach(s => {
    ctx.fillStyle = s.color;
    ctx.fillRect(lx, 4, 12, 3);
    ctx.fillStyle = dimColor;
    ctx.font = '9px Inter'; ctx.textAlign = 'left';
    ctx.fillText(s.label, lx + 16, 10);
    lx += ctx.measureText(s.label).width + 30;
  });
}

// =========================================================================
// STATS
// =========================================================================
function updateStats() {
  const data = DATA[activeRule];
  if (!data) return;
  const stats = (data.stats || [])[currentStep];
  if (!stats) return;

  document.getElementById('stat-nodes').textContent = stats.num_nodes || '--';
  document.getElementById('stat-edges').textContent = stats.num_edges || '--';

  const dim = stats.estimated_dimension;
  document.getElementById('stat-dim').textContent = dim != null ? dim.toFixed(2) : '--';
  document.getElementById('stat-dim-sub').textContent = dim != null ?
    (dim < 1.2 ? '~ 1D structure' : dim < 1.7 ? '~ fractal' : dim < 2.3 ? '~ 2D space' : dim < 3.3 ? '~ 3D space' : 'high-dim') : '';

  const prevStats = currentStep > 0 ? (data.stats || [])[currentStep - 1] : null;
  if (prevStats && prevStats.num_nodes && stats.num_nodes) {
    const rate = (stats.num_nodes / prevStats.num_nodes).toFixed(2);
    document.getElementById('stat-growth').textContent = rate + 'x';
    document.getElementById('stat-growth-sub').textContent = 'vs prev step';
  } else {
    document.getElementById('stat-growth').textContent = '--';
    document.getElementById('stat-growth-sub').textContent = '';
  }
}

// =========================================================================
// CONTROLS
// =========================================================================
function togglePlay() {
  playing = !playing;
  const btn = document.getElementById('play-btn');
  if (playing) {
    btn.innerHTML = '&#9646;&#9646;';
    playTimer = setInterval(() => {
      const data = DATA[activeRule];
      const max = (data.states || []).length - 1;
      if (currentStep >= max) { togglePlay(); return; }
      setStep(currentStep + 1);
    }, playIntervalMs); // §6.9 [L6] uses configurable interval
  } else {
    btn.innerHTML = '&#9654;';
    clearInterval(playTimer);
  }
}

// §6.9 [L6] Play-speed slider handler (50–5000 ms)
function setPlaySpeed(ms) {
  playIntervalMs = ms;
  const display = document.getElementById('speed-display');
  if (display) display.textContent = ms >= 1000 ? (ms / 1000).toFixed(1) + 's' : ms + 'ms';
  if (playing) {
    clearInterval(playTimer);
    playTimer = setInterval(() => {
      const data = DATA[activeRule];
      const max = (data.states || []).length - 1;
      if (currentStep >= max) { togglePlay(); return; }
      setStep(currentStep + 1);
    }, playIntervalMs);
  }
}

function stepPlus1() {
  const data = DATA[activeRule];
  const max = (data.states || []).length - 1;
  if (currentStep < max) setStep(currentStep + 1);
}

function toggleOption(opt) {
  opts[opt] = !opts[opt];
  document.getElementById('toggle-' + opt).classList.toggle('on', opts[opt]);
  if (opt === 'nudge') document.getElementById('nudge-hint').hidden = !opts.nudge;
  renderCurrentView();
}

function showTooltip(event, text) {
  const tip = document.getElementById('tooltip');
  tip.innerHTML = text.replace(/\n/g, '<br>');
  tip.style.display = 'block';
  // Use canvas-area-relative coordinates: clientX/Y minus the canvas rect offset,
  // because #tooltip is position:absolute inside the position:relative #canvas-area.
  const area = document.getElementById('canvas-area').getBoundingClientRect();
  tip.style.left = (event.clientX - area.left + 12) + 'px';
  tip.style.top  = (event.clientY - area.top  -  8) + 'px';
}
function hideTooltip() { document.getElementById('tooltip').style.display = 'none'; }

document.addEventListener('keydown', e => {
  const data = DATA[activeRule];
  if (!data) return;
  const max = (data.states || []).length - 1;
  if (e.key === 'ArrowLeft' && currentStep > 0) setStep(currentStep - 1);
  if (e.key === 'ArrowRight' && currentStep < max) setStep(currentStep + 1);
  if (e.key === ' ') { e.preventDefault(); togglePlay(); }
  if (e.key === 'Escape') clearLineage();
});

// =========================================================================
// RULE EDITOR
// =========================================================================
const VARS = ['x','y','z','w'];
const INIT_VALS = ['0','1','2','3','4'];

let editorLHS = [['x','y'],['x','z']];
let editorRHS = [['x','z'],['x','w'],['y','w'],['z','w']];
let editorInit = [['0','1'],['1','2'],['2','0']];

function initEditor() {
  renderEditorEdges('lhs', editorLHS, VARS);
  renderEditorEdges('rhs', editorRHS, VARS);
  renderEditorEdges('init', editorInit, INIT_VALS);
  updatePreview();
}

function _span(text) {
  const s = document.createElement('span');
  s.className = 'edge-label'; s.textContent = text;
  return s;
}

function renderEditorEdges(side, edges, options) {
  const container = document.getElementById(side + '-edges');
  container.innerHTML = '';
  edges.forEach((edge, ei) => {
    const row = document.createElement('div');
    row.className = 'edge-row';
    row.appendChild(_span('{'));
    edge.forEach((v, vi) => {
      if (vi > 0) row.appendChild(_span(','));
      const sel = document.createElement('select');
      options.forEach(opt => {
        const o = document.createElement('option');
        o.value = opt; o.textContent = opt;
        if (opt === v) o.selected = true;
        sel.appendChild(o);
      });
      sel.onchange = () => { edges[ei][vi] = sel.value; updatePreview(); };
      row.appendChild(sel);
    });
    row.appendChild(_span('}'));

    const addEl = document.createElement('button');
    addEl.className = 'remove-edge'; addEl.textContent = '+'; addEl.title = 'Add element';
    addEl.onclick = () => { if (edge.length < 4) { edge.push(options[0]); renderEditorEdges(side, edges, options); updatePreview(); } };
    row.appendChild(addEl);
    if (edge.length > 2) {
      const remEl = document.createElement('button');
      remEl.className = 'remove-edge'; remEl.textContent = '\u2212'; remEl.title = 'Remove last element';
      remEl.onclick = () => { edge.pop(); renderEditorEdges(side, edges, options); updatePreview(); };
      row.appendChild(remEl);
    }

    if (edges.length > 1) {
      const rem = document.createElement('button');
      rem.className = 'remove-edge'; rem.textContent = '\u00d7'; rem.title = 'Remove edge';
      rem.onclick = () => { edges.splice(ei, 1); renderEditorEdges(side, edges, options); updatePreview(); };
      row.appendChild(rem);
    }
    container.appendChild(row);
  });
}

function addEdge(side) {
  const map = { lhs: editorLHS, rhs: editorRHS, init: editorInit };
  const o = side === 'init' ? INIT_VALS : VARS;
  map[side].push([o[0], o[1]]);
  renderEditorEdges(side, map[side], o);
  updatePreview();
}

function updatePreview() {
  const fmt = edges => '{' + edges.map(e => '{' + e.join(',') + '}').join(',') + '}';
  document.getElementById('custom-preview').textContent = fmt(editorLHS) + ' \u2192 ' + fmt(editorRHS);
}

// =========================================================================
// CUSTOM RULE — calls server POST /api/custom
// =========================================================================
let customRuleCounter = 0;

async function runCustomRule() {
  const btn = document.querySelector('.run-btn');
  const errEl = document.getElementById('custom-error');
  errEl.style.display = 'none';
  btn.disabled = true;
  btn.textContent = 'Computing\u2026';
  btn.style.opacity = '0.7';

  try {
    const steps = Math.min(15, Math.max(1, +document.getElementById('custom-steps').value || 8));
    const fmt = edges => '{' + edges.map(e => '{' + e.join(',') + '}').join(',') + '}';
    const notation = fmt(editorLHS) + ' \u2192 ' + fmt(editorRHS);
    const init = editorInit.map(e => e.map(v => parseInt(v)));

    // POST /api/custom — returns immediately: cached (status:'done') or new job
    const jobResp = await apiFetch('/api/custom', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ notation, init, steps }),
    });

    let result;
    if (jobResp.status === 'done') {
      result = jobResp; // cache hit — full payload already in response
    } else {
      // New job (status:'running') — show overlay and poll GET /api/jobs/{job_id}
      _jobAborted = false; // reset before starting new job
      showComputeOverlay('running', 'Computing…');
      _currentJobId = jobResp.job_id;
      const jobStarted = Date.now();

      // Client-side ticker fills the elapsed display between 2s poll intervals
      const elapsedTicker = setInterval(() => {
        const elEl = document.getElementById('co-elapsed');
        if (elEl) elEl.textContent = ((Date.now() - jobStarted) / 1000).toFixed(1) + 's elapsed';
      }, 1000);

      const finalPoll = await pollJobUntilDone(jobResp.job_id, poll => {
        // Server-authoritative step progress
        const stepMsg = (poll.step > 0 && poll.total_steps > 0)
          ? 'Computing… step ' + poll.step + ' / ' + poll.total_steps
          : 'Computing…';
        document.getElementById('co-msg').textContent = stepMsg;
        const elEl = document.getElementById('co-elapsed');
        if (elEl) elEl.textContent = poll.elapsed_s.toFixed(1) + 's elapsed';
      });

      clearInterval(elapsedTicker);
      _currentJobId = null;

      if (finalPoll.status !== 'done') {
        if (_jobAborted) {
          // abortCurrentJob() already updated the overlay — just restore the button.
          _jobAborted = false;
          btn.textContent = 'Run Simulation';
          btn.style.opacity = '1';
          btn.disabled = false;
          return;
        }
        if (finalPoll.status === 'stale') {
          showComputeOverlay('stale', 'Server restarted mid-compute — click Retry to try again');
        } else {
          showComputeOverlay('error',
            'Computation failed: ' + (finalPoll.error || 'server error') + ' — try again');
        }
        btn.textContent = 'Run Simulation';
        btn.style.opacity = '1';
        btn.disabled = false;
        return;
      }
      result = finalPoll;
      hideComputeOverlay();
    }

    customRuleCounter++;
    const ruleId = 'custom_' + customRuleCounter;
    DATA[ruleId] = {
      states: result.states,
      events: result.events,
      causal_edges: result.causalEdges,
      stats: result.stats,
      lineage: result.lineage,
      birthSteps: result.birthSteps,
      _customParsed: parseNotation(notation),
      _blurb: 'Custom rule: ' + notation,
      // Cache key from server — used by POST /api/extend for subsequent +1 calls.
      _cacheKey: result.key || null,
    };

    if (result.multiway) {
      MULTIWAY[ruleId] = result.multiway;
    }

    const finalNodes = result.stats[result.stats.length - 1].num_nodes;
    const finalEdges = result.stats[result.stats.length - 1].num_edges;
    RULES.push({
      id: ruleId,
      name: 'Custom #' + customRuleCounter,
      notation: notation,
      desc: finalNodes + ' nodes at step ' + steps,
      tag: 'Custom', tagClass: 'tag-custom',
      isCustom: true,
      // §6.8 [L5] blurb stored in rule object; no longer uses BLURBS constant
      blurb: 'Custom rule: ' + notation + '. Evolved for ' + steps + ' steps. Final graph: ' + finalNodes + ' nodes, ' + finalEdges + ' edges.',
    });

    renderRuleCards();
    selectRule(ruleId);

    btn.textContent = '\u2713 Ready \u2014 Run Again';
    btn.style.opacity = '1';
    btn.disabled = false;
  } catch (e) {
    errEl.textContent = e.message;
    errEl.style.display = 'block';
    btn.textContent = 'Run Simulation';
    btn.style.opacity = '1';
    btn.disabled = false;
  }
}

// =========================================================================
// MULTIWAY VIEW
// =========================================================================
function renderMultiway() {
  const mw = MULTIWAY[activeRule];
  const svg = d3.select('#multiway-svg');
  svg.selectAll('*').remove();
  const rect = document.getElementById('canvas-area').getBoundingClientRect();
  const width = rect.width, height = rect.height;
  svg.attr('width', width).attr('height', height);

  if (!mw || !mw.states || Object.keys(mw.states).length < 2) {
    svg.append('text').attr('x', width/2).attr('y', height/2)
      .attr('text-anchor', 'middle').attr('fill', isDark ? '#666' : '#999')
      .attr('font-size', 14).text(mw ? 'Multiway data has fewer than 2 states' : 'Loading multiway system...');
    if (!mw && activeRule && !activeRule.startsWith('custom_')) loadMultiway(activeRule);
    return;
  }

  const g = svg.append('g');
  svg.call(d3.zoom().scaleExtent([0.05, 20]).on('zoom', e => g.attr('transform', e.transform)));

  const nodeMap = {};
  const nodes = [];
  for (const [hash, info] of Object.entries(mw.states)) {
    const n = { id: hash, step: info.step, edgeCount: (info.state || []).length };
    nodes.push(n);
    nodeMap[hash] = n;
  }

  // Build links with curve offsets
  const linkCount = {};
  for (const e of mw.edges) {
    const key = e.from + '|' + e.to;
    linkCount[key] = (linkCount[key] || 0) + 1;
  }
  const links = [];
  for (const key of Object.keys(linkCount)) {
    const [from, to] = [key.substring(0, key.indexOf('|')), key.substring(key.indexOf('|') + 1)];
    const count = linkCount[key];
    for (let i = 0; i < count; i++) {
      const curve = count === 1 ? 0 : (i - (count - 1) / 2) * 25;
      links.push({ source: from, target: to, curve, count, idx: i });
    }
  }

  // Default path
  const defaultPathHashes = mw.defaultPathHashes ? new Set(mw.defaultPathHashes) : new Set([mw.initHash]);
  const defaultPathEdges = new Set();
  for (const me of mw.edges) {
    if (mw.defaultPathEventIds && (Array.isArray(mw.defaultPathEventIds) ? mw.defaultPathEventIds.includes(me.event.id) : mw.defaultPathEventIds.has?.(me.event.id))) {
      defaultPathEdges.add(me.from + '|' + me.to);
    }
  }

  // Selected path
  const selPathHashes = new Set();
  const selPathEdgeKeys = new Set();
  if (selectedMultiwayNode) {
    const path = tracePathTo(mw, selectedMultiwayNode);
    selPathHashes.add(mw.initHash);
    for (const e of path) {
      selPathHashes.add(e.to);
      selPathEdgeKeys.add(e.from + '|' + e.to);
    }
  }

  const maxStep = Math.max(1, ...nodes.map(n => n.step));
  const byStep = {};
  nodes.forEach(n => { (byStep[n.step] = byStep[n.step] || []).push(n); });
  for (const [step, sns] of Object.entries(byStep)) {
    const s = Number(step);
    sns.forEach((n, i) => {
      n.x = width/2 + (i - (sns.length-1)/2) * Math.min(60, (width - 100) / Math.max(1, sns.length));
      n.y = 60 + s * (height - 120) / maxStep;
    });
  }

  const nodeR = Math.max(3, Math.min(8, 200 / Math.sqrt(nodes.length)));

  g.append('defs').append('marker')
    .attr('id', 'mw-arrow').attr('viewBox', '0 -3 6 6').attr('refX', 10)
    .attr('markerWidth', 5).attr('markerHeight', 5).attr('orient', 'auto')
    .append('path').attr('d', 'M0,-3L6,0L0,3').attr('fill', isDark ? '#66668880' : '#88889980');

  g.append('defs').append('marker')
    .attr('id', 'mw-arrow-red').attr('viewBox', '0 -3 6 6').attr('refX', 10)
    .attr('markerWidth', 5).attr('markerHeight', 5).attr('orient', 'auto')
    .append('path').attr('d', 'M0,-3L6,0L0,3').attr('fill', '#ff4444');

  function linkKey(d) {
    const s = typeof d.source === 'object' ? d.source.id : d.source;
    const t = typeof d.target === 'object' ? d.target.id : d.target;
    return s + '|' + t;
  }
  function curvedPath(d) {
    const sn = nodeMap[typeof d.source === 'object' ? d.source.id : d.source];
    const tn = nodeMap[typeof d.target === 'object' ? d.target.id : d.target];
    const sx = sn?.x || 0, sy = sn?.y || 0, tx = tn?.x || 0, ty = tn?.y || 0;
    if (d.curve === 0) return 'M' + sx + ',' + sy + 'L' + tx + ',' + ty;
    const mx = (sx + tx) / 2 + d.curve, my = (sy + ty) / 2;
    return 'M' + sx + ',' + sy + 'Q' + mx + ',' + my + ' ' + tx + ',' + ty;
  }

  g.append('g').selectAll('path').data(links).join('path')
    .attr('d', curvedPath)
    .attr('fill', 'none')
    .attr('stroke', d => {
      const k = linkKey(d);
      if (selPathEdgeKeys.has(k)) return '#44aaff';
      if (defaultPathEdges.has(k)) return '#ff4444';
      return isDark ? '#33335580' : '#88889960';
    })
    .attr('stroke-width', d => {
      const k = linkKey(d);
      return (selPathEdgeKeys.has(k) || defaultPathEdges.has(k)) ? 2 : 0.8;
    })
    .attr('marker-end', d => {
      const k = linkKey(d);
      return (selPathEdgeKeys.has(k) || defaultPathEdges.has(k)) ? 'url(#mw-arrow-red)' : 'url(#mw-arrow)';
    });

  g.append('g').selectAll('circle').data(nodes).join('circle')
    .attr('cx', d => d.x).attr('cy', d => d.y)
    .attr('r', d => {
      if (d.id === mw.initHash) return nodeR * 1.5;
      if (selPathHashes.has(d.id)) return nodeR * 1.3;
      if (defaultPathHashes.has(d.id)) return nodeR * 1.2;
      return nodeR;
    })
    .attr('fill', d => {
      if (d.id === selectedMultiwayNode) return '#44aaff';
      if (selPathHashes.has(d.id)) return '#44aaff80';
      if (defaultPathHashes.has(d.id)) return '#ff4444';
      return d3.interpolateViridis(d.step / maxStep);
    })
    .attr('stroke', d => {
      if (d.id === selectedMultiwayNode) return '#fff';
      if (defaultPathHashes.has(d.id)) return '#ff444480';
      return isDark ? '#08080c' : '#fff';
    })
    .attr('stroke-width', d => (d.id === selectedMultiwayNode || defaultPathHashes.has(d.id)) ? 2 : 0.5)
    .style('cursor', 'pointer')
    .on('mouseenter', (ev, d) => {
      const st = mw.states[d.id];
      showTooltip(ev,
        `Step ${d.step}\n` +
        `${(st.state || []).length} edges, ${new Set((st.state || []).flat()).size} nodes\n` +
        (d.id === mw.initHash ? '(initial state)' : 'Click to select this branch')
      );
    })
    .on('mouseleave', hideTooltip)
    .on('click', (ev, d) => {
      ev.stopPropagation();
      if (d.id === selectedMultiwayNode) {
        selectedMultiwayNode = null;
        selectedPath = null;
      } else {
        selectedMultiwayNode = d.id;
        const pathEdges = tracePathTo(mw, d.id);
        selectedPath = pathEdges.map(e => e.event);
      }
      renderMultiway();
    });

  // Step labels
  for (let s = 0; s <= maxStep; s++) {
    g.append('text').attr('x', 20).attr('y', 60 + s * (height - 120) / maxStep + 4)
      .attr('fill', isDark ? '#666' : '#999').attr('font-size', 11)
      .attr('font-family', 'JetBrains Mono').text(`t=${s}`);
  }

  // Legend
  const lg = g.append('g').attr('transform', `translate(${width - 180}, 20)`);
  lg.append('circle').attr('cx', 0).attr('cy', 0).attr('r', 5).attr('fill', '#ff4444');
  lg.append('text').attr('x', 10).attr('y', 4).attr('fill', isDark ? '#aaa' : '#555')
    .attr('font-size', 11).text('One possible history (greedy)')
    .append('title').text('The greedy rewriting order is one of many valid orderings. ' +
      'All branches are equally valid histories of the same rule.');
  lg.append('circle').attr('cx', 0).attr('cy', 20).attr('r', 5).attr('fill', '#44aaff');
  lg.append('text').attr('x', 10).attr('y', 24).attr('fill', isDark ? '#aaa' : '#555')
    .attr('font-size', 11).text('Selected path');

  g.append('text').attr('x', width/2).attr('y', height - 15)
    .attr('text-anchor', 'middle').attr('fill', isDark ? '#555' : '#999')
    .attr('font-size', 11)
    .text(`${Object.keys(mw.states).length} states, ${mw.edges.length} transitions (${Math.min(4, maxStep)} steps)`);
}

function tracePathTo(mw, targetHash) {
  const parent = {};
  const parentEdge = {};
  const queue = [mw.initHash];
  const visited = new Set([mw.initHash]);

  const children = {};
  for (const e of mw.edges) {
    (children[e.from] = children[e.from] || []).push(e);
  }

  while (queue.length > 0) {
    const cur = queue.shift();
    if (cur === targetHash) break;
    for (const e of (children[cur] || [])) {
      if (!visited.has(e.to)) {
        visited.add(e.to);
        parent[e.to] = cur;
        parentEdge[e.to] = e;
        queue.push(e.to);
      }
    }
  }

  const path = [];
  let cur = targetHash;
  while (cur !== mw.initHash && parent[cur]) {
    path.unshift(parentEdge[cur]);
    cur = parent[cur];
  }
  return path;
}

// =========================================================================
// MINIMAL CLIENT-SIDE ENGINE (for causal view alt-match detection only)
// =========================================================================
const HGEngine = (() => {
  function findMatches(hyp, pattern) {
    const results = [];
    _matchRec(hyp, pattern, 0, [], {}, new Set(), results);
    const seen = new Set();
    return results.filter(([mi, bind]) => {
      const key = mi.join(',') + ':' + JSON.stringify(bind);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }

  function _edgePerms(e) {
    if (e.length <= 1) return [e];
    if (e.length === 2) return [e, [e[1], e[0]]];
    const res = [];
    for (let i = 0; i < e.length; i++) {
      const rest = e.slice(0, i).concat(e.slice(i + 1));
      for (const p of _edgePerms(rest)) res.push([e[i], ...p]);
    }
    return res;
  }

  function _matchRec(hyp, pat, pi, matched, binding, used, results) {
    if (pi === pat.length) { results.push([matched.slice(), {...binding}]); return; }
    const pe = pat[pi];
    for (let i = 0; i < hyp.length; i++) {
      if (used.has(i) || hyp[i].length !== pe.length) continue;
      for (const perm of _edgePerms(hyp[i])) {
        const nb = {...binding};
        let ok = true;
        for (let j = 0; j < pe.length; j++) {
          if (nb[pe[j]] !== undefined) { if (nb[pe[j]] !== perm[j]) { ok = false; break; } }
          else nb[pe[j]] = perm[j];
        }
        if (!ok) continue;
        matched.push(i); used.add(i);
        _matchRec(hyp, pat, pi + 1, matched, nb, used, results);
        used.delete(i); matched.pop();
      }
    }
  }

  return { findMatches };
})();

function parseNotation(notation) {
  const arrow = notation.includes('\u2192') ? '\u2192' : '->';
  if (!notation.includes(arrow)) return null;
  const [lStr, rStr] = notation.split(arrow);
  function parseSide(s) {
    s = s.trim();
    // Strip one layer of outer braces for the multi-edge wrapper:
    //   "{{x,y},{y,z}}" \u2192 "{x,y},{y,z}"  (double-brace: multi-edge)
    //   "{x,y}"         \u2192 "x,y"           (single-brace: single-edge wrapper)
    if (s.startsWith('{{')) s = s.slice(1, -1);
    else if (s.startsWith('{')) s = s.slice(1, -1);
    const edges = [];
    for (const m of s.matchAll(/\{([^}]+)\}/g)) {
      edges.push(m[1].split(',').map(v => v.trim()));
    }
    // Fallback: if no inner {\u2026} were found the side was a single edge whose
    // outer braces we already stripped (e.g. original "{x,y}" \u2192 now "x,y").
    // Treat the entire remaining string as one edge's elements.
    if (edges.length === 0 && s.length > 0) {
      edges.push(s.split(',').map(v => v.trim()).filter(Boolean));
    }
    return edges;
  }
  return { lhs: parseSide(lStr), rhs: parseSide(rStr) };
}

// =========================================================================
// BOOT
// =========================================================================
setTimeout(initEditor, 100);
init();
