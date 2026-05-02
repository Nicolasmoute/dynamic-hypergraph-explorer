/* client/layout-worker.js
 * Web-Worker force-layout engine for the Dynamic Hypergraph Explorer.
 *
 * Protocol version 1 — contract: knowledge/perf/canvas-worker-contract.md §3
 * Contract revision 2 amendments (Phase 1.5):
 *   - graph_id echoed on every outbound message (stale-tick guard)
 *   - Extended force options: centerX/Y, linkDistance/Strength,
 *     chargeStrength/DistanceMax, collisionRadius, hyperedgeMode
 *   - update_graph before init → structured error (not_initialized)
 *   - importScripts wrapped in try/catch for CDN failure detection
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
// Wrapped in try/catch so a CDN failure produces a structured error rather
// than a silent worker crash or missing 'ready' that would hang the main
// thread's ready-timeout handler.

try {
  importScripts('https://d3js.org/d3.v7.min.js');
} catch (loadErr) {
  postMessage({
    type:             'error',
    protocol_version: PROTOCOL_VERSION,
    graph_id:         null,
    reason:           'd3_load_failed',
    detail:           loadErr ? (loadErr.message || String(loadErr)) : 'importScripts failed',
  });
  self.close();
  // Stop executing — throw so nothing below runs
  throw loadErr;
}

postMessage({ type: 'ready', protocol_version: PROTOCOL_VERSION });

// ── worker state ───────────────────────────────────────────────────────────

let _sim         = null;   // d3.forceSimulation
let _nodes       = null;   // [{id, x, y, vx, vy}, …] — d3 mutates these in place
let _initialized = false;
let _frozen      = false;
let _graphId     = null;   // echoed on every outbound message for stale-tick detection

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
  _graphId     = msg.graph_id != null ? msg.graph_id : null;
  _buildSim(
    msg.nodes      || [],
    msg.edges      || [],
    msg.hyperedges || [],
    msg.warmstart  || null,
    msg.options    || {}
  );
}

function _handleUpdateGraph(msg) {
  // Strict lifecycle: update_graph requires a prior init.
  // A main-thread bug that skips init gets an explicit error rather than
  // silently working, which makes lifecycle issues easier to diagnose.
  if (!_initialized) {
    _postError('not_initialized',
      'update_graph received before init — send init first');
    return;
  }
  _frozen  = false;
  _graphId = msg.graph_id != null ? msg.graph_id : null;

  // Capture live positions so they serve as an implicit warm-start for
  // nodes that survive the step change.
  const prevPos = {};
  if (_nodes) {
    for (const n of _nodes) prevPos[n.id] = [n.x, n.y];
  }
  // Caller's explicit warmstart takes precedence over live positions
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
  _graphId     = null;
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
 * Build (or rebuild) the d3-force simulation from raw graph data.
 *
 * @param {Array}       rawNodes       [{id:int}, …]
 * @param {Array}       rawEdges       [{source:int, target:int}, …]
 * @param {Array}       rawHyperedges  [{id:int, nodes:[int]}, …]
 * @param {Object|null} warmstart      {[id]: [x, y]} — pre-placed positions
 * @param {Object}      options        simulation tuning (see below)
 *
 * options fields (all optional):
 *   alpha           {number}  Initial sim alpha (default 0.3)
 *   alphaDecay      {number}  Decay per tick (default 0.0228)
 *   velocityDecay   {number}  Friction (default 0.4)
 *   centerX         {number}  forceCenter X (default 0)
 *   centerY         {number}  forceCenter Y (default 0)
 *   linkDistance    {number}  forceLink target distance px (default 40)
 *   linkStrength    {number}  forceLink strength 0–1 (default 0.5)
 *   chargeStrength  {number}  forceManyBody strength, negative = repel (default -80)
 *   chargeDistanceMax {number} forceManyBody max influence radius (default Infinity)
 *   collisionRadius {number}  forceCollide radius; 0 = disabled (default 0)
 *   hyperedgeMode   {string}  'clique' (default) or 'chain'
 *     'clique' — all pairs of hyperedge members linked (strong cohesion)
 *     'chain'  — sequential pairs only (matches current SVG chain behavior)
 */
function _buildSim(rawNodes, rawEdges, rawHyperedges, warmstart, options) {
  // ── node objects ───────────────────────────────────────────────────────
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

  // ── links ──────────────────────────────────────────────────────────────
  // Binary edges are passed through as direct d3 links.
  const links = [];
  for (const e of rawEdges) {
    const src = nodeIndex.get(e.source);
    const tgt = nodeIndex.get(e.target);
    if (src && tgt) links.push({ source: src, target: tgt });
  }

  // Hyperedges are expanded per hyperedgeMode:
  //   'clique' — O(n²) pairs; strong spatial cohesion for all members
  //   'chain'  — sequential pairs (edge[i]→edge[i+1]); matches current
  //              SVG renderer's forceLink construction for arity > 2
  const hyperedgeMode = options.hyperedgeMode === 'chain' ? 'chain' : 'clique';
  for (const he of rawHyperedges) {
    const members = (he.nodes || [])
      .map(function (id) { return nodeIndex.get(id); })
      .filter(Boolean);
    if (hyperedgeMode === 'chain') {
      for (let i = 0; i < members.length - 1; i++) {
        links.push({ source: members[i], target: members[i + 1] });
      }
    } else {
      for (let i = 0; i < members.length - 1; i++) {
        for (let j = i + 1; j < members.length; j++) {
          links.push({ source: members[i], target: members[j] });
        }
      }
    }
  }

  // ── simulation parameters ──────────────────────────────────────────────
  const alpha           = options.alpha           != null ? +options.alpha           : 0.3;
  const alphaDecay      = options.alphaDecay      != null ? +options.alphaDecay      : 0.0228;
  const velocityDecay   = options.velocityDecay   != null ? +options.velocityDecay   : 0.4;
  const centerX         = options.centerX         != null ? +options.centerX         : 0;
  const centerY         = options.centerY         != null ? +options.centerY         : 0;
  const linkDistance    = options.linkDistance    != null ? +options.linkDistance    : 40;
  const linkStrength    = options.linkStrength    != null ? +options.linkStrength    : 0.5;
  const chargeStrength  = options.chargeStrength  != null ? +options.chargeStrength  : -80;
  const chargeDistMax   = options.chargeDistanceMax != null ? +options.chargeDistanceMax : Infinity;
  const collisionRadius = options.collisionRadius != null ? +options.collisionRadius : 0;

  // ── build sim ──────────────────────────────────────────────────────────
  const chargeForce = d3.forceManyBody()
    .strength(chargeStrength)
    .distanceMax(chargeDistMax);

  const linkForce = d3.forceLink(links)
    .distance(linkDistance)
    .strength(linkStrength);

  _sim = d3.forceSimulation(_nodes)
    .alpha(alpha)
    .alphaDecay(alphaDecay)
    .velocityDecay(velocityDecay)
    .alphaMin(0.001)
    .force('link',   linkForce)
    .force('charge', chargeForce)
    .force('center', d3.forceCenter(centerX, centerY))
    .on('tick', _onTick)
    .on('end',  _onSettled);

  // Collision force is optional — only wire it up if a non-zero radius was given
  if (collisionRadius > 0) {
    _sim.force('collision', d3.forceCollide(collisionRadius));
  }
}

// ── outbound message helpers ───────────────────────────────────────────────

/**
 * Pack current node positions into new transferable typed arrays.
 * Returns { positions: Float32Array(2*N), node_ids: Int32Array(N) }.
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

/**
 * Called by d3 on each simulation tick (~60 Hz).
 * graph_id is echoed so the main thread can discard ticks from a prior
 * step that arrive after update_graph has advanced the active graph.
 */
function _onTick() {
  if (_frozen || !_nodes || _nodes.length === 0) return;
  const { positions, node_ids } = _packPositions();
  postMessage(
    {
      type:             'tick',
      protocol_version: PROTOCOL_VERSION,
      graph_id:         _graphId,
      positions:        positions,
      node_ids:         node_ids,
      alpha:            _sim.alpha(),
    },
    [positions.buffer, node_ids.buffer]   // transfer ownership — zero-copy
  );
}

/**
 * Called by d3 once alpha drops below alphaMin.
 */
function _onSettled() {
  if (!_nodes) return;
  const { positions, node_ids } = _packPositions();
  postMessage(
    {
      type:             'settled',
      protocol_version: PROTOCOL_VERSION,
      graph_id:         _graphId,
      positions:        positions,
      node_ids:         node_ids,
    },
    [positions.buffer, node_ids.buffer]
  );
}

/**
 * Post a structured error message and self-terminate.
 * Per contract §3: worker self-terminates on any internal failure.
 */
function _postError(reason, detail) {
  postMessage({
    type:             'error',
    protocol_version: PROTOCOL_VERSION,
    graph_id:         _graphId,
    reason:           reason,
    detail:           detail || null,
  });
  _destroySim();
  _nodes       = null;
  _initialized = false;
  _graphId     = null;
  self.close();
}
