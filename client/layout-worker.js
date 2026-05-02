/* client/layout-worker.js
 * Web-Worker force-layout engine for the Dynamic Hypergraph Explorer.
 *
 * Protocol version 2 — contract: knowledge/perf/canvas-worker-contract.md §3
 * r2 amendments:
 *   - PROTOCOL_VERSION bumped to 2
 *   - graph_id echoed on every outbound message (stale-tick guard)
 *   - Hyperedge expansion: sequential CHAIN only (clique removed per contract)
 *   - update_options message: live-update force params without sim restart
 *   - Node-count-scaled defaults for linkDistance, chargeStrength, collisionRadius
 *   - selectedEdge option for lineage-mode emphasis
 *   - chargeDistanceMax default = 300
 *   - importScripts try/catch for CDN-failure detection
 *   - update_graph before init → error{reason:"not_initialized"}
 *
 * Classic-script worker (no ESM).  Instantiate from main thread with:
 *   new Worker('client/layout-worker.js')   // no { type:'module' } needed
 *
 * D3 v7 loaded via importScripts (same URL as client/index.html).
 */

'use strict';

// ── constants ──────────────────────────────────────────────────────────────

const PROTOCOL_VERSION = 2;

// ── bootstrap: load D3, then announce readiness ────────────────────────────
// Wrapped in try/catch so a CDN / CSP failure produces a structured error
// message instead of a silent worker crash.

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
  // self.close() schedules termination but does NOT immediately halt
  // execution of the current task. Re-throw so the rest of the module
  // body (postMessage('ready'), state declarations, message router
  // attachment) does not run before termination takes effect. Phase 3
  // observes both the structured `error` message and the implicit
  // worker.onerror that this re-throw triggers — either signal is
  // sufficient to fall back to the SVG renderer.
  throw loadErr;
}

postMessage({ type: 'ready', protocol_version: PROTOCOL_VERSION });

// ── worker state ───────────────────────────────────────────────────────────

let _sim         = null;   // d3.forceSimulation
let _nodes       = null;   // [{id, x, y, vx, vy}, …] — d3 mutates these in place
let _initialized = false;
let _frozen      = false;
let _graphId     = null;   // echoed on every outbound message (stale-tick detection)

// ── message router ─────────────────────────────────────────────────────────

self.onmessage = function (evt) {
  const msg = evt.data;

  // Protocol-version guard on every inbound message
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
      case 'init':           _handleInit(msg);           break;
      case 'update_graph':   _handleUpdateGraph(msg);    break;
      case 'update_options': _handleUpdateOptions(msg);  break;
      case 'reheat':         _handleReheat(msg);          break;
      case 'freeze':         _handleFreeze(msg);          break;
      case 'terminate':      _handleTerminate();          break;
      default:
        _postError('unknown_message_type', 'Unknown type: ' + msg.type);
    }
  } catch (err) {
    _postError('internal_error', err ? (err.message || String(err)) : 'unknown');
  }
};

// ── graph_id validation helpers ────────────────────────────────────────────

/**
 * Per contract r2 §3, all messages except `terminate` (and the outbound
 * `ready`) carry a required, non-empty `graph_id`. Returns true if valid;
 * otherwise posts a structured `missing_graph_id` error and self-terminates,
 * and returns false.
 */
function _requireGraphId(msg, msgType) {
  if (msg.graph_id == null || msg.graph_id === '') {
    _postError('missing_graph_id',
      msgType + ' requires a non-empty graph_id (contract r2 §3)');
    return false;
  }
  return true;
}

/**
 * Returns true if the message's graph_id matches the worker's currently
 * active graph. Used by update_options / reheat / freeze to silently
 * drop stale operations from a previous step / rule (no error — main
 * thread races are common and benign).
 */
function _isCurrentGraph(msg) {
  return msg.graph_id === _graphId;
}

// ── message handlers ───────────────────────────────────────────────────────

function _handleInit(msg) {
  if (!_requireGraphId(msg, 'init')) return;
  if (_initialized) {
    _postError('double_init', 'init received on already-initialized worker');
    return;
  }
  _initialized = true;
  _frozen      = false;
  _graphId     = msg.graph_id;
  _buildSim(
    msg.nodes      || [],
    msg.edges      || [],
    msg.hyperedges || [],
    msg.warmstart  || null,
    msg.options    || {}
  );
}

function _handleUpdateGraph(msg) {
  if (!_requireGraphId(msg, 'update_graph')) return;
  // Strict lifecycle: update_graph requires a prior init.
  if (!_initialized) {
    _postError('not_initialized',
      'update_graph received before init — send init first');
    return;
  }
  _frozen  = false;
  _graphId = msg.graph_id;

  // Snapshot live positions as implicit warm-start for surviving node ids
  const prevPos = {};
  if (_nodes) {
    for (const n of _nodes) prevPos[n.id] = [n.x, n.y];
  }
  // Caller's explicit warmstart takes precedence
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

/**
 * Live-update force parameters without restarting the simulation.
 * e.g. user moved a force slider, viewport resized (center changed),
 * or an edge became selected/deselected.
 * No alpha change; no position reset; simulation continues from current state.
 *
 * Stale-graph operations (msg.graph_id !== _graphId) are silently ignored
 * — main thread races on step transitions are common; an error would be
 * disruptive. Missing graph_id IS an error (programming bug, not a race).
 */
function _handleUpdateOptions(msg) {
  if (!_requireGraphId(msg, 'update_options')) return;
  if (!_initialized || !_sim) return;       // not running — caller may race on startup
  if (!_isCurrentGraph(msg))    return;     // stale graph — silently drop
  _applyForceOptions(msg.options || {});
}

function _handleReheat(msg) {
  if (!_requireGraphId(msg, 'reheat')) return;
  if (!_sim)                 return;
  if (!_isCurrentGraph(msg)) return;        // stale graph — silently drop
  _frozen = false;
  _sim.alpha(msg.alpha != null ? +msg.alpha : 0.3).restart();
}

function _handleFreeze(msg) {
  if (!_requireGraphId(msg, 'freeze')) return;
  if (!_isCurrentGraph(msg))           return;  // stale graph — silently drop
  _frozen = true;
  if (_sim) _sim.stop();
}

function _handleTerminate() {
  _destroySim();
  _nodes       = null;
  _initialized = false;
  _graphId     = null;
  self.close();
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
 * Hyperedge expansion: SEQUENTIAL CHAIN only — (n0,n1), (n1,n2), …
 * Per contract r2, clique expansion is out of scope.
 *
 * @param {Array}       rawNodes       [{id:int}, …]
 * @param {Array}       rawEdges       [{source:int, target:int, id?:int}, …]
 * @param {Array}       rawHyperedges  [{id:int, nodes:[int]}, …]
 * @param {Object|null} warmstart      {[id]: [x, y]}
 * @param {Object}      options        see _resolveOpts() for fields
 */
function _buildSim(rawNodes, rawEdges, rawHyperedges, warmstart, options) {
  // ── node objects ──────────────────────────────────────────────────────
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
  const N = _nodes.length;

  // ── links ─────────────────────────────────────────────────────────────
  // Binary edges: direct d3 links.  Carry _link_id for selectedEdge matching.
  const links = [];
  for (const e of rawEdges) {
    const src = nodeIndex.get(e.source);
    const tgt = nodeIndex.get(e.target);
    if (!src || !tgt) continue;
    links.push({ source: src, target: tgt, _link_id: e.id != null ? e.id : null });
  }

  // Hyperedges → sequential-chain pairs (n0,n1), (n1,n2), …
  // Each pair carries the hyperedge's id for selectedEdge matching.
  for (const he of rawHyperedges) {
    const members = (he.nodes || [])
      .map(function (id) { return nodeIndex.get(id); })
      .filter(Boolean);
    for (let i = 0; i < members.length - 1; i++) {
      links.push({ source: members[i], target: members[i + 1], _link_id: he.id });
    }
  }

  // ── forces ────────────────────────────────────────────────────────────
  const opts = _resolveOpts(options, N);

  const linkForce = d3.forceLink(links)
    .distance(_linkDistanceFn(opts))
    .strength(_linkStrengthFn(opts));

  const chargeForce = d3.forceManyBody()
    .strength(opts.chargeStrength)
    .distanceMax(opts.chargeDistMax);

  _sim = d3.forceSimulation(_nodes)
    .alpha(opts.alpha)
    .alphaDecay(opts.alphaDecay)
    .velocityDecay(opts.velocityDecay)
    .alphaMin(0.001)
    .force('link',   linkForce)
    .force('charge', chargeForce)
    .force('center', d3.forceCenter(opts.centerX, opts.centerY))
    .on('tick', _onTick)
    .on('end',  _onSettled);

  if (opts.collisionRadius > 0) {
    _sim.force('collision', d3.forceCollide(opts.collisionRadius));
  }
}

/**
 * Live-apply option changes to the running simulation.
 * Called by update_options; also used internally by _buildSim via
 * the forces already set.
 */
function _applyForceOptions(options) {
  const N    = _nodes ? _nodes.length : 0;
  const opts = _resolveOpts(options, N);

  const lf = _sim.force('link');
  if (lf) {
    lf.distance(_linkDistanceFn(opts));
    lf.strength(_linkStrengthFn(opts));
  }

  const cf = _sim.force('charge');
  if (cf) cf.strength(opts.chargeStrength).distanceMax(opts.chargeDistMax);

  const cenf = _sim.force('center');
  if (cenf) cenf.x(opts.centerX).y(opts.centerY);

  if (opts.collisionRadius > 0) {
    const colf = _sim.force('collision');
    if (colf) colf.radius(opts.collisionRadius);
    else _sim.force('collision', d3.forceCollide(opts.collisionRadius));
  } else {
    _sim.force('collision', null);
  }

  // Sim-level knobs
  if (options.alphaDecay    != null) _sim.alphaDecay(+options.alphaDecay);
  if (options.velocityDecay != null) _sim.velocityDecay(+options.velocityDecay);
}

// ── option helpers ─────────────────────────────────────────────────────────

/**
 * Resolve raw options into concrete numbers, applying node-count scaling
 * where a field is absent (§3.1 default scaling formulas).
 */
function _resolveOpts(options, N) {
  const safeN = Math.max(N, 1);
  return {
    alpha:           options.alpha           != null ? +options.alpha           : 1,
    alphaDecay:      options.alphaDecay      != null ? +options.alphaDecay      : 0.0228,
    velocityDecay:   options.velocityDecay   != null ? +options.velocityDecay   : 0.4,
    centerX:         options.centerX         != null ? +options.centerX         : 0,
    centerY:         options.centerY         != null ? +options.centerY         : 0,
    // §3.1 default scaling — matches client/app.js:821-835
    linkDistance:    options.linkDistance    != null ? +options.linkDistance
                       : 40 * Math.sqrt(Math.min(1, 100 / safeN)),
    linkStrength:    options.linkStrength    != null ? +options.linkStrength    : 0.5,
    chargeStrength:  options.chargeStrength  != null ? +options.chargeStrength
                       : -80 * (N <= 100 ? 1 : 100 / safeN),
    chargeDistMax:   options.chargeDistanceMax != null ? +options.chargeDistanceMax : 300,
    collisionRadius: options.collisionRadius != null ? +options.collisionRadius
                       : (N >= 50 ? 5 : 0),
    selectedEdge:    options.selectedEdge    || null,
  };
}

/** Return a d3-forceLink distance accessor that handles selectedEdge. */
function _linkDistanceFn(opts) {
  const base    = opts.linkDistance;
  const sel     = opts.selectedEdge;
  if (!sel) return base;
  return function (link) {
    return (link._link_id != null && link._link_id === sel.edge_id)
      ? +sel.distance
      : base;
  };
}

/** Return a d3-forceLink strength accessor that handles selectedEdge. */
function _linkStrengthFn(opts) {
  const base = opts.linkStrength;
  const sel  = opts.selectedEdge;
  if (!sel) return base;
  return function (link) {
    return (link._link_id != null && link._link_id === sel.edge_id)
      ? +sel.strength
      : base;
  };
}

// ── outbound message helpers ───────────────────────────────────────────────

/**
 * Pack current node positions into new transferable typed arrays.
 * Allocates fresh arrays each call — callers transfer the buffers.
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
      graph_id:         _graphId,
      positions:        positions,
      node_ids:         node_ids,
      alpha:            _sim.alpha(),
    },
    [positions.buffer, node_ids.buffer]
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
      graph_id:         _graphId,
      positions:        positions,
      node_ids:         node_ids,
    },
    [positions.buffer, node_ids.buffer]
  );
}

/**
 * Post a structured error and self-terminate.
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
