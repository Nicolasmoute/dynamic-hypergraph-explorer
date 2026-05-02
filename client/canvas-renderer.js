// =============================================================================
// Canvas Renderer — Dynamic Hypergraph Explorer (Phase 2)
// Draws nodes / edges / hulls / labels to a <canvas> element.
// app.js drives this via CanvasRenderer.drawFrame() on every simulation tick.
// Phase 3 will replace tick-driven calls with worker-posted positions.
// =============================================================================

'use strict';

const CanvasRenderer = (() => {
  // ── State ──────────────────────────────────────────────────────────────────
  let _canvas   = null;
  let _ctx      = null;
  let _dpr      = 1;
  let _width    = 0;
  let _height   = 0;
  let _transform = { x: 0, y: 0, k: 1 };  // current d3 zoom transform

  // LEVER-4: hull throttle — recompute polygonHull every N frames, cache results
  let _hullCache         = [];   // [{hull, color}] per hyperedge (null if skipped)
  let _hullFrameCount    = 0;
  const _HULL_RECOMPUTE_EVERY = 5;

  // ── Helpers ────────────────────────────────────────────────────────────────
  // Convert a hex colour + alpha to an rgba() string.  Handles 3- and 6-char hex.
  function _rgba(hex, alpha) {
    if (!hex) return `rgba(128,128,128,${alpha})`;
    const h = hex.replace('#', '');
    const len = h.length;
    const r = parseInt(len === 3 ? h[0] + h[0] : h.slice(0, 2), 16);
    const g = parseInt(len === 3 ? h[1] + h[1] : h.slice(2, 4), 16);
    const b = parseInt(len === 3 ? h[2] + h[2] : h.slice(4, 6), 16);
    return `rgba(${r},${g},${b},${alpha})`;
  }

  // ── Public API ─────────────────────────────────────────────────────────────

  /** Call once when the canvas element is available. */
  function init(canvasEl) {
    _canvas = canvasEl;
    _ctx    = canvasEl.getContext('2d');
    _dpr    = window.devicePixelRatio || 1;
  }

  /**
   * Size the canvas to the given CSS dimensions (accounts for DPR).
   * Call on first render and whenever the container resizes.
   */
  function resize(width, height) {
    _width  = width;
    _height = height;
    _dpr    = window.devicePixelRatio || 1;
    _canvas.width  = Math.round(width  * _dpr);
    _canvas.height = Math.round(height * _dpr);
    _canvas.style.width  = width  + 'px';
    _canvas.style.height = height + 'px';
  }

  /** Store the d3 zoom transform so drawFrame() can apply it. */
  function setTransform(t) {
    _transform = { x: t.x, y: t.y, k: t.k };
  }

  /**
   * Draw one complete frame.
   *
   * @param {object} args
   *   nodes         – array of {id, x, y, _idx, _fill}
   *   nodeById      – Map<id, node>
   *   links         – array of {source, target, edgeIdx, _curve, _stroke}
   *   selfLoops     – array of {node, edgeIdx, _loopIdx, _stroke}
   *   hyperedges    – array of {id, nodes[], _color}
   *   nodeR         – node circle radius
   *   baseEdgeWidth – base stroke width for edges
   *   isDark        – boolean (dark theme)
   *   opts          – {colors, hulls, labels}
   *   selectedEdges – array of {edgeIdx, step}
   *   getEdgeSelColor(edgeIdx) → hex | null
   */
  function drawFrame(args) {
    const ctx = _ctx;
    const { nodes, nodeById, links, selfLoops, hyperedges,
            nodeR, baseEdgeWidth, isDark, opts,
            selectedEdges, getEdgeSelColor } = args;

    // ── Reset transform: DPR scale → clear → zoom/pan ──────────────────────
    ctx.setTransform(1, 0, 0, 1, 0, 0);
    ctx.scale(_dpr, _dpr);
    ctx.clearRect(0, 0, _width, _height);

    const t = _transform;
    ctx.translate(t.x, t.y);
    ctx.scale(t.k, t.k);

    // ── Drawing order (back to front): hulls → edges → loops → nodes → labels
    if (opts.hulls)  _drawHulls(ctx, hyperedges, nodeById, selectedEdges);
                     _drawEdges(ctx, links, baseEdgeWidth, isDark, selectedEdges, getEdgeSelColor);
                     _drawSelfLoops(ctx, selfLoops, nodeById, nodeR, baseEdgeWidth, isDark, selectedEdges, getEdgeSelColor);
                     _drawNodes(ctx, nodes, links, nodeR, isDark, selectedEdges, getEdgeSelColor);
    if (opts.labels) _drawLabels(ctx, nodes, nodeR, isDark);
  }

  // ── Private drawing routines ───────────────────────────────────────────────

  function _drawHulls(ctx, hyperedges, nodeById, selectedEdges) {
    // LEVER-4: recompute convex hulls only every _HULL_RECOMPUTE_EVERY frames.
    // Between recomputes, draw from _hullCache (positions have moved <1px visually).
    const recompute = (_hullFrameCount % _HULL_RECOMPUTE_EVERY) === 0;
    _hullFrameCount++;

    if (recompute) {
      // Ensure cache length matches hyperedges array
      if (_hullCache.length !== hyperedges.length) _hullCache = new Array(hyperedges.length).fill(null);
      for (let i = 0; i < hyperedges.length; i++) {
        const h = hyperedges[i];
        if (h.nodes.length <= 2) { _hullCache[i] = null; continue; }
        const pts = h.nodes.map(nid => {
          const n = nodeById.get(nid);
          return (n && n.x != null) ? [n.x, n.y] : null;
        }).filter(Boolean);
        if (pts.length < 3) { _hullCache[i] = null; continue; }
        const hull = d3.polygonHull(pts);
        _hullCache[i] = hull ? { hull, color: h._color || '#888' } : null;
      }
    }

    // Draw from cache
    for (let i = 0; i < _hullCache.length; i++) {
      const entry = _hullCache[i];
      if (!entry) continue;
      const { hull, color } = entry;
      ctx.beginPath();
      ctx.moveTo(hull[0][0], hull[0][1]);
      for (let j = 1; j < hull.length; j++) ctx.lineTo(hull[j][0], hull[j][1]);
      ctx.closePath();
      ctx.fillStyle   = _rgba(color, 0.06);
      ctx.fill();
      ctx.strokeStyle = _rgba(color, 0.15);
      ctx.lineWidth   = 1;
      ctx.stroke();
    }
  }

  /** Force hull recompute on the next drawFrame call (e.g. after step change). */
  function invalidateHullCache() {
    _hullFrameCount = 0;
    _hullCache      = [];
  }

  function _drawEdges(ctx, links, baseEdgeWidth, isDark, selectedEdges, getEdgeSelColor) {
    for (const d of links) {
      const sx = d.source.x, sy = d.source.y;
      const tx = d.target.x, ty = d.target.y;
      if (sx == null || tx == null) continue;

      const selColor = selectedEdges.length > 0 ? getEdgeSelColor(d.edgeIdx) : null;
      const stroke   = selColor != null ? selColor : (d._stroke || (isDark ? '#3a3a5e' : '#8888aa'));
      const opacity  = selectedEdges.length > 0 ? (selColor != null ? 1 : 0.25) : 0.65;
      const lw       = selColor != null ? baseEdgeWidth * 2 : baseEdgeWidth;

      ctx.beginPath();
      ctx.strokeStyle = _rgba(stroke, opacity);
      ctx.lineWidth   = lw;

      if (d._curve === 0) {
        ctx.moveTo(sx, sy);
        ctx.lineTo(tx, ty);
      } else {
        const mx = (sx + tx) / 2, my = (sy + ty) / 2;
        const dx = tx - sx, dy = ty - sy;
        const len = Math.sqrt(dx * dx + dy * dy) || 1;
        const nx = -dy / len, ny = dx / len;
        const cx = mx + nx * d._curve, cy = my + ny * d._curve;
        ctx.moveTo(sx, sy);
        ctx.quadraticCurveTo(cx, cy, tx, ty);
      }
      ctx.stroke();
    }
  }

  function _drawSelfLoops(ctx, selfLoops, nodeById, nodeR, baseEdgeWidth, isDark, selectedEdges, getEdgeSelColor) {
    for (const sl of selfLoops) {
      const n = nodeById.get(sl.node);
      if (!n || n.x == null) continue;
      const r = nodeR * 3 + sl._loopIdx * nodeR * 2.5;

      const selColor = selectedEdges.length > 0 ? getEdgeSelColor(sl.edgeIdx) : null;
      const stroke   = selColor != null ? selColor : (sl._stroke || (isDark ? '#3a3a5e' : '#8888aa'));
      const opacity  = selectedEdges.length > 0 ? (selColor != null ? 1 : 0.25) : 0.65;
      const lw       = selColor != null ? baseEdgeWidth * 2 : baseEdgeWidth;

      // Use Path2D with the same SVG arc formula for pixel-exact parity
      const p = new Path2D(
        `M${n.x},${n.y - nodeR} A${r},${r} 0 1,1 ${n.x + 0.01},${n.y - nodeR}`
      );
      ctx.strokeStyle = _rgba(stroke, opacity);
      ctx.lineWidth   = lw;
      ctx.stroke(p);
    }
  }

  function _drawNodes(ctx, nodes, links, nodeR, isDark, selectedEdges, getEdgeSelColor) {
    for (let i = 0; i < nodes.length; i++) {
      const d = nodes[i];
      if (d.x == null) continue;

      let fill, opacity;
      if (selectedEdges.length > 0) {
        let best = null;
        for (const l of links) {
          const nid = typeof l.source === 'object' ? l.source.id : l.source;
          const tid = typeof l.target === 'object' ? l.target.id : l.target;
          if (nid === d.id || tid === d.id) {
            const c = getEdgeSelColor(l.edgeIdx);
            if (c != null) { best = c; break; }
          }
        }
        fill    = best != null ? best : (isDark ? '#222' : '#ccc');
        opacity = best != null ? 1 : 0.2;
      } else {
        fill    = d._fill || (isDark ? '#4a4a7a' : '#9999cc');
        opacity = 1;
      }

      ctx.globalAlpha = opacity;
      ctx.beginPath();
      ctx.arc(d.x, d.y, nodeR, 0, 2 * Math.PI);
      ctx.fillStyle   = fill;
      ctx.fill();
      ctx.strokeStyle = isDark ? '#08080c' : '#ffffff';
      ctx.lineWidth   = 0.3;
      ctx.stroke();
    }
    ctx.globalAlpha = 1;
  }

  function _drawLabels(ctx, nodes, nodeR, isDark) {
    const fontSize = Math.max(6, 8 - nodes.length / 100);
    ctx.font        = `${fontSize}px 'JetBrains Mono', monospace`;
    ctx.fillStyle   = isDark ? '#888' : '#666';
    ctx.textAlign   = 'left';
    ctx.textBaseline = 'middle';
    for (const d of nodes) {
      if (d.x == null) continue;
      ctx.fillText(String(d.id), d.x + nodeR + 2, d.y + 3);
    }
  }

  // ── Exported interface ─────────────────────────────────────────────────────
  return { init, resize, setTransform, drawFrame, invalidateHullCache };
})();
