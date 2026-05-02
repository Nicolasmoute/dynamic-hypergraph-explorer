/* client/layout-worker.js
 * Web-Worker force-layout engine for the Dynamic Hypergraph Explorer.
 *
 * Protocol version 1 — contract: knowledge/perf/canvas-worker-contract.md §3
 *
 * Classic-script worker (no ESM).  Instantiate from main thread with:
 *   new Worker('client/layout-worker.js')   // no { type:'module' } needed
 *
 * D3 v7 loaded via importScripts (same URL as client/index.html).
 */

'use strict';

// ── constants ──────────────────────────────────────────────────────────────

const PROTOCOL_VERSION = 1;

// ── bootstrap: load D3, then announce readiness ────────────────────────────

importScripts('https://d3js.org/d3.v7.min.js');

postMessage({ type: 'ready', protocol_version: PROTOCOL_VERSION });

// ── worker state ───────────────────────────────────────────────────────────

let _sim         = null;   // d3.forceSimulation
let _nodes       = null;   // [{id, x, y, vx, vy}, …] — d3 mutates these
let _initialized = false;
let _frozen      = false;

// ── message router ─────────────────────────────────────────────────────────

self.onmessage = function (evt) {
  const msg = evt.data;

  // Protocol-version guard — every message must carry protocol_version
  if (!msg || msg.protocol_version !== PROTOCOL_VERSION) {
    _postError(
      'protocol_version_mismatch',
      'Expected protocol_version ' + PROTOCOL_VERSION +
      ', received ' + (msg ? msg.protocol_version : '(null)')
    );
    return;
  }

  try {
    switch (msg.type) {
      case 'init':         _handleInit(msg);        break;
      case 'update_graph': _handleUpdateGraph(msg); break;
      case 'reheat':       _handleReheat(msg);       break;
      case 'freeze':       _handleFreeze();          break;
      case 'terminate':    _handleTerminate();       break;
      default:
        _postError('unknown_message_type', 'Unknown type: ' + msg.type);
    }
  } catch (err) {
    _postError('internal_error', err ? (err.message || String(err)) : 'unknown');
  }
};

// ── message handlers ───────────────────────────────────────────────────────

function _handleInit(msg) {
  if (_initialized) {
    _postError('double_init', 'init received on already-initialized worker');
    return;
  }
  _initialized = true;
  _frozen      = false;
  _buildSim(
    msg.nodes      || [],
    msg.edges      || [],
    msg.hyperedges || [],
    msg.warmstart  || null,
    msg.options    || {}
  );
}

function _handleUpdateGraph(msg) {
  // update_graph before a formal init is treated as an implicit init
  if (!_initialized) _initialized = true;
  _frozen = false;

  // Capture live positions so they can serve as an implicit warm-start
  const prevPos = {};
  if (_nodes) {
    for (const n of _nodes) prevPos[n.id] = [n.x, n.y];
  }
  // Caller's explicit warmstart wins over live positions
  const warmstart = Object.assign({}, prevPos, msg.warmstart || {});

  _destroySim();
  _buildSim(
    msg.nodes      || [],
    msg.edges      || [],
    msg.hyperedges || [],
    warmstart,
    msg.options    || {}
  );
}

function _handleReheat(msg) {
  if (!_sim) return;
  _frozen = false;
  _sim.alpha(msg.alpha != null ? +msg.alpha : 0.3).restart();
}

function _handleFreeze() {
  _frozen = true;
  if (_sim) _sim.stop();
}

function _handleTerminate() {
  _destroySim();
  _nodes       = null;
  _initialized = false;
  self.close();   // release the worker thread
}

// ── simulation lifecycle ───────────────────────────────────────────────────

function _destroySim() {
  if (_sim) {
    _sim.stop();
    _sim.on('tick', null);
    _sim.on('end',  null);
    _sim = null;
  }
}

/**
 * Build (or rebuild) the d3-force simulation from scratch.
 *
 * @param {Array}  rawNodes      [{id:int}, …]
 * @param {Array}  rawEdges      [{source:int, target:int}, …]
 * @param {Array}  rawHyperedges [{id:int, nodes:[int]}, …]
 * @param {Object|null} warmstart  {[id]: [x, y]}  — pre-placed positions
 * @param {Object} options        optional sim parameters
 */
function _buildSim(rawNodes, rawEdges, rawHyperedges, warmstart, options) {
  // Build mutable node objects; apply warm-start where available
  _nodes = rawNodes.map(function (n) {
    const pos = warmstart && warmstart[n.id];
    return {
      id: n.id,
      x:  pos ? +pos[0] : (Math.random() - 0.5) * 300,
      y:  pos ? +pos[1] : (Math.random() - 0.5) * 300,
      vx: 0,
      vy: 0,
    };
  });

  const nodeIndex = new Map(_nodes.map(function (n) { return [n.id, n]; }));

  // Binary edges → direct d3 links
  const links = [];
  for (const e of rawEdges) {
    const src = nodeIndex.get(e.source);
    const tgt = nodeIndex.get(e.target);
    if (src && tgt) links.push({ source: src, target: tgt });
  }

  // Hyperedges → clique-expansion (every pair of member nodes gets a link).
  // This keeps all member nodes spatially cohesive without a custom force.
  for (const he of rawHyperedges) {
    const members = (he.nodes || [])
      .map(function (id) { return nodeIndex.get(id); })
      .filter(Boolean);
    for (let i = 0; i < members.length - 1; i++) {
      for (let j = i + 1; j < members.length; j++) {
        links.push({ source: members[i], target: members[j] });
      }
    }
  }

  // Simulation parameters — caller may override defaults
  const alpha         = options.alpha         != null ? +options.alpha         : 0.3;
  const alphaDecay    = options.alphaDecay    != null ? +options.alphaDecay    : 0.0228;
  const velocityDecay = options.velocityDecay != null ? +options.velocityDecay : 0.4;

  _sim = d3.forceSimulation(_nodes)
    .alpha(alpha)
    .alphaDecay(alphaDecay)
    .velocityDecay(velocityDecay)
    .alphaMin(0.001)
    .force('link',   d3.forceLink(links).distance(40).strength(0.5))
    .force('charge', d3.forceManyBody().strength(-80))
    .force('center', d3.forceCenter(0, 0))
    .on('tick', _onTick)
    .on('end',  _onSettled);
}

// ── outbound message helpers ───────────────────────────────────────────────

/**
 * Pack current node positions into transferable typed arrays.
 * Returns { positions: Float32Array(2*N), node_ids: Int32Array(N) }.
 * The arrays are new allocations — callers transfer their buffers.
 */
function _packPositions() {
  const N         = _nodes.length;
  const positions = new Float32Array(2 * N);
  const node_ids  = new Int32Array(N);
  for (let i = 0; i < N; i++) {
    positions[2 * i]     = _nodes[i].x;
    positions[2 * i + 1] = _nodes[i].y;
    node_ids[i]          = _nodes[i].id;
  }
  return { positions, node_ids };
}

/** Called by d3 on each simulation tick (~60 Hz). */
function _onTick() {
  if (_frozen || !_nodes || _nodes.length === 0) return;
  const { positions, node_ids } = _packPositions();
  postMessage(
    {
      type:             'tick',
      protocol_version: PROTOCOL_VERSION,
      positions:        positions,
      node_ids:         node_ids,
      alpha:            _sim.alpha(),
    },
    [positions.buffer, node_ids.buffer]   // transfer ownership — zero-copy
  );
}

/** Called by d3 once alpha drops below alphaMin. */
function _onSettled() {
  if (!_nodes) return;
  const { positions, node_ids } = _packPositions();
  postMessage(
    {
      type:             'settled',
      protocol_version: PROTOCOL_VERSION,
      positions:        positions,
      node_ids:         node_ids,
    },
    [positions.buffer, node_ids.buffer]
  );
}

/**
 * Post an error message and self-terminate.
 * Per contract §3: worker self-terminates on any internal failure.
 */
function _postError(reason, detail) {
  postMessage({
    type:             'error',
    protocol_version: PROTOCOL_VERSION,
    reason:           reason,
    detail:           detail || null,
  });
  _destroySim();
  _nodes       = null;
  _initialized = false;
  self.close();
}
