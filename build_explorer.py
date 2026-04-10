"""Build the Dynamic Hypergraph Explorer HTML with embedded data."""
import os, json

tmp = os.environ.get('TEMP')

# Load simulation data
with open(os.path.join(tmp, 'extracted_data.json'), 'r', encoding='utf-8') as f:
    data_str = f.read()

HTML = r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Dynamic Hypergraph Explorer</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600&family=Inter:wght@300;400;500;600;700&display=swap');

  :root {
    --bg: #08080c;
    --surface: #0e0e14;
    --surface2: #14141e;
    --border: #1c1c2e;
    --border-bright: #2a2a4e;
    --text: #c8c8d4;
    --text-dim: #666680;
    --text-heading: #ffffff;
    --accent: #6c7bff;
    --accent-glow: #6c7bff40;
    --green: #4cdd8a;
    --orange: #ffaa44;
    --pink: #ff6b9d;
    --cyan: #4cdddd;
    --node-stroke: #08080c;
    --chart-bg: #0e0e14;
    --chart-grid: #1c1c2e;
  }

  html.light {
    --bg: #eaebef;
    --surface: #ffffff;
    --surface2: #e2e3e8;
    --border: #c8cad2;
    --border-bright: #a0a4b0;
    --text: #2a2d35;
    --text-dim: #5a5e6a;
    --text-heading: #111318;
    --accent: #3a44b8;
    --accent-glow: #3a44b840;
    --green: #117a42;
    --orange: #a86200;
    --pink: #b8306a;
    --cyan: #0e7a7a;
    --node-stroke: #ffffff;
    --chart-bg: #ffffff;
    --chart-grid: #d0d2d8;
  }

  body {
    font-family: 'Inter', -apple-system, sans-serif;
    background: var(--bg);
    color: var(--text);
    height: 100vh;
    overflow: hidden;
    display: flex;
    flex-direction: column;
    transition: background 0.3s, color 0.3s;
  }

  header {
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    padding: 12px 24px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    flex-shrink: 0;
    z-index: 100;
    transition: background 0.3s;
  }
  .logo { display: flex; align-items: center; gap: 12px; }
  .logo-icon {
    width: 32px; height: 32px;
    background: linear-gradient(135deg, var(--accent), var(--pink));
    border-radius: 8px;
    display: flex; align-items: center; justify-content: center;
    font-size: 16px;
  }
  .logo h1 { font-size: 15px; font-weight: 600; color: var(--text-heading); letter-spacing: -0.3px; }
  .logo span { font-size: 11px; color: var(--text-dim); font-weight: 400; }

  .header-right { display: flex; align-items: center; gap: 16px; }

  .view-tabs {
    display: flex; gap: 2px; background: var(--surface2); border-radius: 8px; padding: 3px;
  }
  .view-tab {
    padding: 6px 16px; border-radius: 6px; cursor: pointer;
    font-size: 12px; font-weight: 500; color: var(--text-dim);
    border: none; background: none; transition: all 0.2s;
  }
  .view-tab:hover { color: var(--text); }
  .view-tab.active { background: var(--accent); color: #fff; }

  /* Theme toggle */
  .theme-btn {
    width: 32px; height: 32px; border-radius: 8px; border: 1px solid var(--border);
    background: var(--surface2); cursor: pointer; font-size: 15px;
    display: flex; align-items: center; justify-content: center;
    transition: all 0.2s; color: var(--text);
  }
  .theme-btn:hover { border-color: var(--accent); }

  .main { display: flex; flex: 1; overflow: hidden; }

  .sidebar {
    width: 320px;
    background: var(--surface);
    border-right: 1px solid var(--border);
    display: flex; flex-direction: column;
    overflow-y: auto; flex-shrink: 0;
    transition: background 0.3s;
  }
  .sidebar-section { padding: 16px; border-bottom: 1px solid var(--border); }
  .sidebar-section h3 {
    font-size: 10px; text-transform: uppercase; letter-spacing: 1.2px;
    color: var(--text-dim); margin-bottom: 12px; font-weight: 600;
  }

  .rule-card {
    background: var(--surface2); border: 1px solid var(--border);
    border-radius: 8px; padding: 12px; margin-bottom: 8px;
    cursor: pointer; transition: all 0.2s;
  }
  .rule-card:hover { border-color: var(--border-bright); }
  .rule-card.active { border-color: var(--accent); box-shadow: 0 0 12px var(--accent-glow); }
  .rule-card .rule-name { font-size: 13px; font-weight: 600; color: var(--text-heading); margin-bottom: 4px; }
  .rule-card .rule-notation {
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px; color: var(--accent); margin-bottom: 6px; word-break: break-all;
  }
  .rule-card .rule-desc { font-size: 11px; color: var(--text-dim); line-height: 1.5; }
  .rule-tag { display: inline-block; padding: 2px 6px; border-radius: 4px; font-size: 9px; font-weight: 600; margin-top: 6px; }
  .tag-2d { background: #4cdd8a20; color: var(--green); }
  .tag-1d { background: #ffaa4420; color: var(--orange); }
  .tag-tree { background: #ff6b9d20; color: var(--pink); }
  .tag-fractal { background: #cb7cff20; color: var(--accent); }
  .tag-mixed { background: #ffcc4420; color: #ccaa00; }
  .tag-custom { background: #4cdddd20; color: var(--cyan); }
  .remove-custom {
    float: right; background: none; border: none; color: var(--text-dim);
    font-size: 18px; cursor: pointer; padding: 0 2px; line-height: 1;
    margin: -4px -4px 0 0;
  }
  .remove-custom:hover { color: #e05050; }

  .step-control { display: flex; align-items: center; gap: 12px; margin-top: 12px; }
  .step-control input[type=range] {
    flex: 1; accent-color: var(--accent);
    -webkit-appearance: none; height: 4px; border-radius: 2px; background: var(--border);
  }
  .step-control input[type=range]::-webkit-slider-thumb {
    -webkit-appearance: none; width: 14px; height: 14px;
    border-radius: 50%; background: var(--accent); cursor: pointer;
  }
  .step-value {
    font-family: 'JetBrains Mono', monospace;
    font-size: 13px; font-weight: 600; color: var(--accent); min-width: 24px; text-align: right;
  }

  .stats-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
  .stat-box {
    background: var(--surface2); border: 1px solid var(--border);
    border-radius: 6px; padding: 10px; transition: background 0.3s;
  }
  .stat-box .stat-label { font-size: 9px; text-transform: uppercase; letter-spacing: 0.8px; color: var(--text-dim); margin-bottom: 4px; }
  .stat-box .stat-value { font-family: 'JetBrains Mono', monospace; font-size: 18px; font-weight: 600; color: var(--text-heading); }
  .stat-box .stat-sub { font-size: 10px; color: var(--text-dim); margin-top: 2px; }

  .option-row { display: flex; align-items: center; justify-content: space-between; padding: 6px 0; }
  .option-row label { font-size: 12px; }
  .toggle {
    width: 36px; height: 20px; border-radius: 10px;
    background: var(--border); cursor: pointer; position: relative;
    transition: background 0.2s; border: none;
  }
  .toggle::after {
    content: ''; position: absolute; top: 2px; left: 2px;
    width: 16px; height: 16px; border-radius: 50%;
    background: var(--text-dim); transition: all 0.2s;
  }
  .toggle.on { background: var(--accent); }
  .toggle.on::after { left: 18px; background: #fff; }

  .canvas-area { flex: 1; position: relative; overflow: hidden; }
  .canvas-area svg { width: 100%; height: 100%; }

  .play-btn {
    background: var(--accent); border: none;
    width: 32px; height: 32px; border-radius: 50%; color: #fff;
    cursor: pointer; display: flex; align-items: center; justify-content: center;
    font-size: 14px; transition: all 0.2s; flex-shrink: 0;
  }
  .play-btn:hover { transform: scale(1.1); box-shadow: 0 0 16px var(--accent-glow); }

  .causal-overlay { display: none; position: absolute; top: 0; left: 0; right: 0; bottom: 0; }
  .causal-overlay.active { display: block; }

  .tooltip {
    position: absolute; pointer-events: none;
    background: var(--surface); border: 1px solid var(--border-bright);
    border-radius: 6px; padding: 8px 12px; font-size: 11px;
    font-family: 'JetBrains Mono', monospace;
    line-height: 1.7; max-width: 400px;
    display: none; z-index: 200;
    box-shadow: 0 4px 16px rgba(0,0,0,0.3);
  }

  /* Lineage info bar */
  .lineage-bar {
    position: absolute; top: 12px; left: 50%; transform: translateX(-50%);
    background: var(--surface); border: 1px solid var(--accent);
    border-radius: 8px; padding: 8px 16px; font-size: 12px;
    display: none; z-index: 150; gap: 12px; align-items: center;
    box-shadow: 0 4px 20px var(--accent-glow);
  }
  .lineage-bar.active { display: flex; }
  .lineage-bar .lineage-text { color: var(--accent); font-weight: 500; }
  .lineage-bar .lineage-count { font-family: 'JetBrains Mono', monospace; color: var(--text-heading); font-weight: 600; }
  .lineage-bar button {
    background: none; border: 1px solid var(--border); border-radius: 4px;
    color: var(--text-dim); padding: 2px 8px; cursor: pointer; font-size: 11px;
  }
  .lineage-bar button:hover { border-color: var(--accent); color: var(--accent); }

  ::-webkit-scrollbar { width: 6px; }
  ::-webkit-scrollbar-track { background: transparent; }
  ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }

  /* Rule editor */
  .edge-row {
    display: flex; align-items: center; gap: 4px; margin-bottom: 4px;
  }
  .edge-row select {
    background: var(--surface2); border: 1px solid var(--border); color: var(--text);
    border-radius: 4px; padding: 3px 4px; font-size: 11px;
    font-family: 'JetBrains Mono', monospace; width: 40px; cursor: pointer;
  }
  .edge-row .edge-label {
    font-family: 'JetBrains Mono', monospace; font-size: 10px; color: var(--text-dim);
  }
  .edge-row .remove-edge {
    background: none; border: none; color: var(--text-dim); cursor: pointer;
    font-size: 14px; padding: 0 2px; line-height: 1;
  }
  .edge-row .remove-edge:hover { color: #e05050; }
  .editor-btn {
    background: var(--surface2); border: 1px solid var(--border); color: var(--text-dim);
    padding: 3px 10px; border-radius: 4px; cursor: pointer; font-size: 10px;
    margin-top: 4px;
  }
  .editor-btn:hover { border-color: var(--accent); color: var(--accent); }
  .run-btn {
    display: block; width: 100%; margin-top: 12px; padding: 8px;
    background: var(--accent); border: none; color: #fff; border-radius: 6px;
    font-size: 12px; font-weight: 600; cursor: pointer; transition: all 0.2s;
  }
  .run-btn:hover { filter: brightness(1.2); }
  .run-btn:disabled { opacity: 0.5; cursor: not-allowed; }
</style>
</head>
<body>

<header>
  <div class="logo">
    <div class="logo-icon">&#x2B21;</div>
    <div>
      <h1>Dynamic Hypergraph Explorer</h1>
      <span>Hypergraph Rewriting Simulator</span>
    </div>
  </div>
  <div class="header-right">
    <div class="view-tabs">
      <button class="view-tab active" onclick="setView('spatial', this)">Spatial Graph</button>
      <button class="view-tab" onclick="setView('causal', this)">Causal Graph</button>
      <button class="view-tab" onclick="setView('multiway', this)">Multiway</button>
      <button class="view-tab" onclick="setView('growth', this)">Growth Analysis</button>
    </div>
    <button class="theme-btn" id="theme-btn" onclick="toggleTheme()" title="Toggle light/dark mode">&#9790;</button>
  </div>
</header>

<div class="main">
  <div class="sidebar">
    <div class="sidebar-section">
      <h3>Rewriting Rules</h3>
      <div id="rule-cards"></div>
    </div>
    <div class="sidebar-section">
      <h3>Custom Rule Editor</h3>
      <div id="rule-editor" style="font-size: 11px;">
        <div style="margin-bottom: 8px; color: var(--text-dim);">LHS (input pattern)</div>
        <div id="lhs-edges"></div>
        <button class="editor-btn" onclick="addEdge('lhs')">+ Add LHS edge</button>
        <div style="margin: 10px 0 8px; color: var(--text-dim);">RHS (replacement)</div>
        <div id="rhs-edges"></div>
        <button class="editor-btn" onclick="addEdge('rhs')">+ Add RHS edge</button>
        <div style="margin: 10px 0 8px; color: var(--text-dim);">Initial hypergraph</div>
        <div id="init-edges"></div>
        <button class="editor-btn" onclick="addEdge('init')">+ Add init edge</button>
        <div style="margin-top: 10px; display: flex; align-items: center; gap: 8px;">
          <label style="color: var(--text-dim); white-space:nowrap;">Steps:</label>
          <input type="number" id="custom-steps" value="8" min="1" max="15"
            style="width:48px; background:var(--surface2); border:1px solid var(--border); color:var(--text); border-radius:4px; padding:3px 6px; font-size:11px; font-family: 'JetBrains Mono', monospace;">
        </div>
        <button class="run-btn" onclick="runCustomRule()">Run Simulation</button>
        <div id="custom-error" style="color:#e05050; font-size:10px; margin-top:4px; display:none;"></div>
        <div id="custom-preview" style="font-family:'JetBrains Mono',monospace; font-size:10px; color:var(--accent); margin-top:6px; word-break:break-all;"></div>
      </div>
    </div>
    <div class="sidebar-section">
      <h3>Evolution Step</h3>
      <div class="step-control">
        <button class="play-btn" id="play-btn" onclick="togglePlay()">&#9654;</button>
        <input type="range" id="step-slider" min="0" max="20" value="0" oninput="setStep(+this.value)">
        <span class="step-value" id="step-display">0</span>
        <button class="play-btn" style="width:26px;height:26px;font-size:16px;font-weight:700;" onclick="stepPlus1()">+</button>
      </div>
    </div>
    <div class="sidebar-section">
      <h3>Statistics</h3>
      <div class="stats-grid">
        <div class="stat-box"><div class="stat-label">Nodes</div><div class="stat-value" id="stat-nodes">1</div></div>
        <div class="stat-box"><div class="stat-label">Edges</div><div class="stat-value" id="stat-edges">2</div></div>
        <div class="stat-box"><div class="stat-label">Dimension</div><div class="stat-value" id="stat-dim">--</div><div class="stat-sub" id="stat-dim-sub"></div></div>
        <div class="stat-box"><div class="stat-label">Growth Rate</div><div class="stat-value" id="stat-growth">--</div><div class="stat-sub" id="stat-growth-sub"></div></div>
      </div>
    </div>
    <div class="sidebar-section">
      <h3>Display Options</h3>
      <div class="option-row"><label>Node labels</label><button class="toggle" id="toggle-labels" onclick="toggleOption('labels')"></button></div>
      <div class="option-row"><label>Edge colors</label><button class="toggle on" id="toggle-colors" onclick="toggleOption('colors')"></button></div>
      <div class="option-row"><label>Hyperedge hulls</label><button class="toggle on" id="toggle-hulls" onclick="toggleOption('hulls')"></button></div>
      <div class="option-row"><label>Nudge mode</label><button class="toggle" id="toggle-nudge" onclick="toggleOption('nudge')"></button></div>
      <div style="font-size:10px; color:var(--text-dim); margin-top:4px; line-height:1.5;" id="nudge-hint" hidden>Click &amp; drag on empty space to push nearby nodes around</div>
    </div>
    <div class="sidebar-section" style="border-bottom: none;">
      <h3>About This Rule</h3>
      <p id="theory-blurb" style="font-size: 11px; line-height: 1.7; color: var(--text-dim);"></p>
    </div>
  </div>

  <div class="canvas-area" id="canvas-area">
    <svg id="main-svg"></svg>
    <div class="causal-overlay" id="causal-view"><svg id="causal-svg"></svg></div>
    <div class="causal-overlay" id="multiway-view"><svg id="multiway-svg"></svg></div>
    <div id="growth-view" style="display:none; position:absolute; inset:0; background:var(--bg); overflow-y:auto; padding: 32px;"></div>
    <div class="lineage-bar" id="lineage-bar">
      <span class="lineage-text">Edge lineage:</span>
      <span class="lineage-count" id="lineage-count">0 descendants</span>
      <button onclick="clearLineage()">&#10005; Clear</button>
    </div>
    <div class="tooltip" id="tooltip"></div>
  </div>
</div>

<script src="https://d3js.org/d3.v7.min.js"></script>
<script>
// =========================================================================
// RULES CONFIG
// =========================================================================
const RULES = [
  {
    id: 'rule1',
    name: 'Signature Rule',
    notation: '{{x,y},{x,z}} \u2192 {{x,z},{x,w},{y,w},{z,w}}',
    desc: 'Two edges sharing a source become four, creating a new node. Generates emergent 2D space.',
    tag: '2D Space', tagClass: 'tag-2d',
    blurb: 'Starting from two self-loops on a single node, this rule generates a hypergraph whose large-scale structure approximates continuous 2-dimensional space. The estimated dimension converges toward 2.0 as the graph grows. Each rule application finds two edges sharing a common source node and replaces them with four edges plus a fresh node \u2014 the fundamental act of "space creation."'
  },
  {
    id: 'rule2',
    name: 'Path Subdivision',
    notation: '{{x,y},{y,z}} \u2192 {{x,y},{y,w},{w,z}}',
    desc: 'Finds a 2-edge path and inserts a new node in the middle. Refines 1D ring structure.',
    tag: '1D Ring', tagClass: 'tag-1d',
    blurb: 'This rule finds any path of two consecutive edges (x\u2192y\u2192z) and subdivides it by inserting a fresh node w. Starting from a pentagon, it progressively refines the ring into a finer and finer circular mesh. The dimension converges to ~1.0, confirming it generates 1-dimensional space.'
  },
  {
    id: 'rule3',
    name: 'Binary Tree Growth',
    notation: '{{x,y}} \u2192 {{x,y},{y,z}}',
    desc: 'Every edge spawns a new edge from its target. Exponential doubling every step.',
    tag: 'Exponential', tagClass: 'tag-tree',
    blurb: 'The simplest non-trivial rule: every edge {x,y} keeps itself and adds a new edge {y,z} from its target to a fresh node. This produces a binary tree with perfect exponential doubling \u2014 2^n edges at step n. This rule illustrates how even the simplest rewriting generates complex structure from nothing.'
  },
  {
    id: 'rule4',
    name: 'Sierpinski Hyperedge',
    notation: '{{x,y,z}} \u2192 {{x,u,w},{y,v,u},{z,w,v}}',
    desc: 'Ternary hyperedges split into 3 new ones. Fractal structure, dimension \u2248 1.4.',
    tag: 'Fractal', tagClass: 'tag-fractal',
    blurb: 'This rule operates on ternary hyperedges \u2014 relations connecting 3 nodes at once. Each hyperedge {x,y,z} is replaced by three new hyperedges, each introducing fresh nodes u, v, w. Starting from a single triangle-relation {0,1,2}, it generates a Sierpinski-like fractal with dimension converging to ~1.4. This demonstrates how hyperedges (not just binary edges) create fundamentally different spatial structures.'
  },
  {
    id: 'rule5',
    name: 'Chain Rewrite',
    notation: '{{x,y,z},{z,u,v}} \u2192 {{y,z,u},{v,w,x},{w,y,v}}',
    desc: 'Two chain-linked ternary hyperedges fuse into three. Unchained edges survive across steps.',
    tag: 'Partial', tagClass: 'tag-mixed',
    blurb: 'This rule matches two ternary hyperedges linked end-to-start (the last node of the first equals the first node of the second). Both are consumed and replaced by three new hyperedges with a fresh node. Starting from an interlocking mesh of 8 ternary hyperedges, each step leaves 5\u201327% of edges unmatched \u2014 they survive with their birth-step color while the active portion reshapes. Demonstrates partial rewriting on a genuinely evolving hypergraph.'
  }
];

let DATA = {};
let activeRule = 'rule1';
let currentStep = 0;
let currentView = 'spatial';
let playing = false;
let playTimer = null;
let simulation = null;
let isDark = true;

// Options
let opts = { labels: false, colors: true, hulls: true, nudge: false };

// Lineage tracking — multi-select up to 3 edges
const SEL_COLORS = ['#ffdd00', '#ff2222', '#2288ff']; // yellow, red, blue
const SEL_MIX = {
  '100': '#ffdd00', '010': '#ff2222', '001': '#2288ff',       // single
  '110': '#ff8800', '101': '#44dd44', '011': '#cc44ff',       // pairs: orange, green, purple
  '111': '#ffffff'                                              // all three: white
};
let selectedEdges = [];      // array of {edgeIdx, step} up to 3
let lineageSets = [];        // parallel array of Sets of descendant edge indices
let edgeLineageMap = {};     // edgeIdx -> set of descendant edge indices

function getEdgeSelColor(edgeIdx) {
  // Returns the mixed color for an edge based on which selections it belongs to
  const bits = [0, 0, 0];
  for (let i = 0; i < selectedEdges.length; i++) {
    const sel = selectedEdges[i];
    const isOrigin = currentStep === sel.step && edgeIdx === sel.edgeIdx;
    const isDesc = lineageSets[i] && lineageSets[i].has(edgeIdx);
    if (isOrigin || isDesc) bits[i] = 1;
  }
  const key = bits.join('');
  if (key === '000') return null; // not part of any selection
  return SEL_MIX[key] || null;
}

// Multiway system
let MULTIWAY = {};  // ruleId -> { states: {hash: edges}, edges: [{from,to,event}], paths: {hash: [event_ids]} }
let selectedMultiwayNode = null;  // hash of selected state in multiway view
let selectedPath = null;  // array of event objects along selected path

// Palettes — dark uses bright neons, light uses saturated deep colors
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
  // Re-render current view to update colors
  renderCurrentView();
}

// =========================================================================
// INIT
// =========================================================================
async function init() {
  DATA = window.__HYPERGRAPH_DATA__;

  // Re-evolve each rule using the JS engine (undirected matching)
  // Only re-evolve early steps where multi-edges matter; keep precomputed data for later steps
  for (const r of RULES) {
    const d = DATA[r.id];
    if (!d || !d.states || !d.states[0]) continue;
    const parsed = parseNotation(r.notation);
    if (!parsed) continue;
    const maxReevolve = Math.min(d.states.length - 1, 6);
    try {
      const result = HGEngine.evolve(d.states[0], parsed.lhs, parsed.rhs, maxReevolve);
      for (let i = 0; i < result.states.length && i < d.states.length; i++) {
        d.states[i] = result.states[i];
      }
      for (let i = 0; i < result.events.length && i < d.events.length; i++) {
        d.events[i] = result.events[i];
      }
    } catch(e) { console.warn('Re-evolve failed for ' + r.id, e); }
  }

  buildLineageMaps();
  renderRuleCards();
  selectRule('rule1');
  // Precompute multiway for all preloaded rules in background
  setTimeout(() => {
    for (const r of RULES) {
      if (!MULTIWAY[r.id]) computeMultiway(r.id);
    }
  }, 200);
}

// =========================================================================
// LINEAGE: build parent->child edge maps from causal event data
// =========================================================================
function buildLineageMaps() {
  // For each rule, build a map: for each edge in any state,
  // which edges in the NEXT state did it produce?
  // We use the events data: each event consumed edges and produced edges.
  // An edge in step N that was consumed by an event produces the event's produced edges in step N+1.
  for (const ruleId of Object.keys(DATA)) {
    const data = DATA[ruleId];
    if (!data.events || !data.states) continue;

    data._edgeLineage = {}; // key: "step:edgeIdx" -> array of "step+1:edgeIdx"

    for (let step = 0; step < data.events.length; step++) {
      const prevState = data.states[step];
      const nextState = data.states[step + 1];
      if (!nextState) continue;

      for (const ev of data.events[step]) {
        // Find which edge indices in prevState were consumed
        const consumedIndices = [];
        for (const consumed of ev.consumed) {
          for (let i = 0; i < prevState.length; i++) {
            if (JSON.stringify(prevState[i]) === JSON.stringify(consumed) && !consumedIndices.includes(i)) {
              consumedIndices.push(i);
              break;
            }
          }
        }
        // Find which edge indices in nextState were produced
        const producedIndices = [];
        const usedNext = new Set();
        for (const produced of ev.produced) {
          for (let i = 0; i < nextState.length; i++) {
            if (!usedNext.has(i) && JSON.stringify(nextState[i]) === JSON.stringify(produced)) {
              producedIndices.push(i);
              usedNext.add(i);
              break;
            }
          }
        }
        // Map consumed -> produced
        for (const ci of consumedIndices) {
          const key = step + ':' + ci;
          if (!data._edgeLineage[key]) data._edgeLineage[key] = [];
          for (const pi of producedIndices) {
            data._edgeLineage[key].push((step + 1) + ':' + pi);
          }
        }
      }

      // Edges that survived (not consumed) map to themselves in next state
      const allConsumed = new Set();
      for (const ev of data.events[step]) {
        for (const consumed of ev.consumed) {
          for (let i = 0; i < prevState.length; i++) {
            if (!allConsumed.has(i) && JSON.stringify(prevState[i]) === JSON.stringify(consumed)) {
              allConsumed.add(i);
              break;
            }
          }
        }
      }
      // Surviving edges: find their position in nextState
      const usedNextSurv = new Set();
      for (let i = 0; i < prevState.length; i++) {
        if (allConsumed.has(i)) continue;
        const key = step + ':' + i;
        for (let j = 0; j < nextState.length; j++) {
          if (!usedNextSurv.has(j) && JSON.stringify(prevState[i]) === JSON.stringify(nextState[j])) {
            if (!data._edgeLineage[key]) data._edgeLineage[key] = [];
            data._edgeLineage[key].push((step + 1) + ':' + j);
            usedNextSurv.add(j);
            break;
          }
        }
      }
    }

    // Build birth step map: for each edge at each step, when was it first created?
    // Step 0: all edges born at step 0
    data._edgeBirthStep = [];
    if (data.states.length > 0) {
      data._edgeBirthStep[0] = data.states[0].map(() => 0);
    }
    for (let step = 1; step < data.states.length; step++) {
      const births = new Array(data.states[step].length).fill(step); // default: born this step
      // Check lineage: edges that are descendants of a prev-step edge inherit its birth
      for (let pi = 0; pi < (data.states[step - 1] || []).length; pi++) {
        const key = (step - 1) + ':' + pi;
        const children = data._edgeLineage[key] || [];
        for (const child of children) {
          const [cStep, cIdx] = child.split(':').map(Number);
          if (cStep === step) {
            // This edge in the current step came from pi in prev step
            // If the prev edge survived (same content), inherit birth step
            const prevBirth = (data._edgeBirthStep[step - 1] || [])[pi];
            const prevEdge = data.states[step - 1][pi];
            const curEdge = data.states[step][cIdx];
            if (prevBirth !== undefined &&
                JSON.stringify(prevEdge) === JSON.stringify(curEdge)) {
              // Surviving edge — keep original birth step
              births[cIdx] = prevBirth;
            }
            // Produced edges (different content) keep births[cIdx] = step (new)
          }
        }
      }
      data._edgeBirthStep[step] = births;
    }
  }
}

function getDescendants(ruleId, step, edgeIdx) {
  const data = DATA[ruleId];
  if (!data._edgeLineage) return new Set();

  // BFS forward through lineage
  const result = new Set();
  const queue = [step + ':' + edgeIdx];
  const visited = new Set(queue);

  while (queue.length > 0) {
    const current = queue.shift();
    const children = data._edgeLineage[current] || [];
    for (const child of children) {
      if (!visited.has(child)) {
        visited.add(child);
        // Only include descendants at the CURRENT viewing step
        const [cStep, cIdx] = child.split(':').map(Number);
        if (cStep === currentStep) {
          result.add(cIdx);
        } else if (cStep < currentStep) {
          queue.push(child);
        }
      }
    }
  }
  return result;
}

function recomputeLineage() {
  lineageSets = selectedEdges.map(sel => {
    window._lineageOriginStep = sel.step;
    return getDescendantsFromStep(activeRule, currentStep, sel.edgeIdx);
  });
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
  container.innerHTML = RULES.map(r => `
    <div class="rule-card ${r.id === activeRule ? 'active' : ''}" id="card-${r.id}" onclick="selectRule('${r.id}')">
      ${r.isCustom ? '<button class="remove-custom" onclick="event.stopPropagation(); removeCustomRule(\'' + r.id + '\')" title="Remove">&times;</button>' : ''}
      <div class="rule-name">${r.name}</div>
      <div class="rule-notation">${r.notation}</div>
      <div class="rule-desc">${r.desc}</div>
      <span class="rule-tag ${r.tagClass}">${r.tag}</span>
    </div>
  `).join('');
}

function removeCustomRule(ruleId) {
  const idx = RULES.findIndex(r => r.id === ruleId);
  if (idx === -1) return;
  RULES.splice(idx, 1);
  delete DATA[ruleId];
  if (activeRule === ruleId) selectRule('rule1');
  renderRuleCards();
}

function selectRule(ruleId) {
  activeRule = ruleId;
  clearLineage();
  document.querySelectorAll('.rule-card').forEach(c => c.classList.remove('active'));
  document.getElementById('card-' + ruleId).classList.add('active');

  const data = DATA[ruleId];
  const maxStep = (data.states || []).length - 1;
  const slider = document.getElementById('step-slider');
  slider.max = maxStep;
  slider.value = Math.min(currentStep, maxStep);
  currentStep = +slider.value;

  const rule = RULES.find(r => r.id === ruleId);
  document.getElementById('theory-blurb').textContent = rule.blurb;
  selectedMultiwayNode = null;
  selectedPath = null;
  if (!MULTIWAY[ruleId]) {
    setTimeout(() => { computeMultiway(ruleId); if (currentView === 'multiway') renderMultiway(); }, 50);
  }
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
  document.getElementById('causal-view').className = 'causal-overlay' + (view === 'causal' ? ' active' : '');
  document.getElementById('multiway-view').className = 'causal-overlay' + (view === 'multiway' ? ' active' : '');
  document.getElementById('growth-view').style.display = view === 'growth' ? '' : 'none';
  if (view !== 'spatial') document.getElementById('lineage-bar').classList.remove('active');
  renderCurrentView();
}

function renderCurrentView() {
  if (currentView === 'spatial') renderSpatial();
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
  // Recompute lineage for all selections
  if (selectedEdges.length > 0) {
    recomputeLineage();
  }
  renderCurrentView();
}

// =========================================================================
// SPATIAL GRAPH
// =========================================================================
function renderSpatial() {
  const data = DATA[activeRule];
  if (!data || !data.states || currentStep >= data.states.length) return;
  // If a multiway state is selected, show that instead
  const mw = MULTIWAY[activeRule];
  const state = (selectedMultiwayNode && mw && mw.states[selectedMultiwayNode])
    ? mw.states[selectedMultiwayNode].state
    : data.states[currentStep];

  const svg = d3.select('#main-svg');
  svg.selectAll('*').remove();
  // Clear all custom event handlers from previous render
  svg.on('mousedown.nudge', null).on('mousemove.nudge', null).on('mouseup.nudge', null);
  const rect = document.getElementById('canvas-area').getBoundingClientRect();
  const width = rect.width, height = rect.height;

  const g = svg.append('g');
  const zoomBehavior = d3.zoom().scaleExtent([0.05, 20]).on('zoom', e => g.attr('transform', e.transform));
  svg.call(zoomBehavior);
  // When nudge is on, disable mouse-drag panning (keep scroll-to-zoom)
  if (opts.nudge) svg.on('mousedown.zoom', null);

  // Click on background to clear lineage
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
  // Assign curve offsets for parallel (multi) edges between same node pair
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
  const nodes = Array.from(nodeSet).map(id => ({ id }));

  // SMALLER nodes, THICKER edges
  const nodeR = Math.max(0.5, Math.min(2, 60 / Math.sqrt(nodes.length)));
  const baseEdgeWidth = Math.max(0.8, 2.5 - nodes.length / 400);

  // Birth step color: edges colored by the step they were created
  const birthSteps = (data._edgeBirthStep && data._edgeBirthStep[currentStep]) || [];
  const maxBirthStep = Math.max(1, currentStep);
  function edgeBirthColor(edgeIdx) {
    const birth = birthSteps[edgeIdx] !== undefined ? birthSteps[edgeIdx] : 0;
    return getPalette()[birth % getPalette().length];
  }

  const hullG = g.append('g');

  // Links — clickable for lineage (paths for multi-edge curvature)
  const link = g.append('g').selectAll('path').data(links).join('path')
    .attr('fill', 'none')
    .attr('stroke', (d) => {
      if (selectedEdges.length > 0) {
        const selColor = getEdgeSelColor(d.edgeIdx);
        if (selColor) return selColor;
        return opts.colors ? edgeBirthColor(d.edgeIdx) : (isDark ? '#3a3a5e' : '#8888aa');
      }
      return opts.colors ? edgeBirthColor(d.edgeIdx) : (isDark ? '#3a3a5e' : '#8888aa');
    })
    .attr('stroke-width', (d) => {
      if (selectedEdges.length > 0) {
        const selColor = getEdgeSelColor(d.edgeIdx);
        if (selColor) return baseEdgeWidth * 2;
        return baseEdgeWidth;
      }
      return baseEdgeWidth;
    })
    .attr('stroke-opacity', (d) => {
      if (selectedEdges.length > 0) {
        const selColor = getEdgeSelColor(d.edgeIdx);
        if (selColor) return 1;
        return 0.25;
      }
      return 0.65;
    })
    .style('cursor', 'pointer')
    .on('click', function(e, d) {
      e.stopPropagation();
      // Toggle: if already selected, remove it; otherwise add (up to 3)
      const existIdx = selectedEdges.findIndex(s => s.step === currentStep && s.edgeIdx === d.edgeIdx);
      if (existIdx >= 0) {
        selectedEdges.splice(existIdx, 1);
        lineageSets.splice(existIdx, 1);
      } else {
        if (selectedEdges.length >= 3) {
          // Remove oldest selection to make room
          selectedEdges.shift();
          lineageSets.shift();
        }
        selectedEdges.push({ edgeIdx: d.edgeIdx, step: currentStep });
      }

      if (selectedEdges.length === 0) {
        clearLineage();
        return;
      }
      recomputeLineage();
      renderSpatial();
    });

  // Compute node birth step: earliest birth step of any edge containing that node
  const nodeBirthMap = {};
  state.forEach((edge, idx) => {
    const birth = birthSteps[idx] !== undefined ? birthSteps[idx] : 0;
    edge.forEach(n => {
      if (nodeBirthMap[n] === undefined || birth < nodeBirthMap[n]) {
        nodeBirthMap[n] = birth;
      }
    });
  });
  function nodeBirthColor(nodeId) {
    const birth = nodeBirthMap[nodeId] !== undefined ? nodeBirthMap[nodeId] : 0;
    return getPalette()[birth % getPalette().length];
  }

  // Self-loops: draw as small circular arcs
  const loopG = g.append('g');

  // Nodes
  const node = g.append('g').selectAll('circle').data(nodes).join('circle')
    .attr('r', nodeR)
    .attr('fill', (d, i) => {
      if (selectedEdges.length > 0) {
        // Find best color from any link touching this node
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
    .attr('opacity', (d) => {
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

  // Labels
  const labelG = g.append('g');
  if (opts.labels) {
    labelG.selectAll('text').data(nodes).join('text')
      .attr('font-size', Math.max(6, 8 - nodes.length/100))
      .attr('font-family', 'JetBrains Mono, monospace')
      .attr('fill', isDark ? '#888' : '#666')
      .attr('dx', nodeR + 2).attr('dy', 3)
      .text(d => d.id);
  }

  if (simulation) simulation.stop();
  const baseDist = 20 + 200/Math.sqrt(nodes.length);
  const linkDistFn = d => {
    if (selectedEdges.length > 0 && getEdgeSelColor(d.edgeIdx)) return baseDist * 0.2;
    return baseDist;
  };
  const linkStrFn = d => {
    if (selectedEdges.length > 0 && getEdgeSelColor(d.edgeIdx)) return 1.0;
    return 0.3;
  };
  simulation = d3.forceSimulation(nodes)
    .force('link', d3.forceLink(links).id(d => d.id).distance(linkDistFn).strength(linkStrFn))
    .force('charge', d3.forceManyBody().strength(-30 - 2000/nodes.length).distanceMax(300))
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
      if (opts.labels) labelG.selectAll('text').attr('x',d=>d.x).attr('y',d=>d.y);
      if (opts.hulls) drawHulls(hullG, hyperedges, nodes, edgeBirthColor);
      // Self-loops — offset multiple loops on the same node
      loopG.selectAll('path').remove();
      const loopCount = {};
      selfLoops.forEach(sl => { loopCount[sl.node] = (loopCount[sl.node] || 0) + 1; });
      const loopIdx = {};
      selfLoops.forEach((sl, i) => {
        const n = nodes.find(x => x.id === sl.node);
        if (!n || n.x == null) return;
        const idx = loopIdx[sl.node] || 0;
        loopIdx[sl.node] = idx + 1;
        const r = nodeR * 3 + idx * nodeR * 2.5;
        loopG.append('path')
          .attr('d', `M${n.x},${n.y - nodeR} A${r},${r} 0 1,1 ${n.x + 0.01},${n.y - nodeR}`)
          .attr('fill', 'none')
          .attr('stroke', () => {
            if (selectedEdges.length > 0) {
              const c = getEdgeSelColor(sl.edgeIdx);
              if (c) return c;
            }
            return opts.colors ? edgeBirthColor(sl.edgeIdx) : (isDark ? '#3a3a5e' : '#8888aa');
          })
          .attr('stroke-width', selectedEdges.length > 0 && getEdgeSelColor(sl.edgeIdx) ? baseEdgeWidth * 2 : baseEdgeWidth)
          .attr('stroke-opacity', selectedEdges.length > 0 ? (getEdgeSelColor(sl.edgeIdx) ? 1 : 0.25) : 0.65);
      });
    });

  // Nudge mode: drag on empty space to push nearby nodes
  if (opts.nudge) {
    svg.style('cursor', 'crosshair');
    const nudgeRadius = 140;
    const nudgeStrength = 2.0;
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
  } else {
    svg.on('mousedown.nudge', null).on('mousemove.nudge', null).on('mouseup.nudge', null);
  }
}

// Get descendants from a specific step (not just step 0)
function getDescendantsFromStep(ruleId, viewingStep, edgeIdx) {
  const originStep = window._lineageOriginStep;
  const data = DATA[ruleId];
  if (!data._edgeLineage) return new Set();

  const result = new Set();
  const queue = [originStep + ':' + edgeIdx];
  const visited = new Set(queue);

  while (queue.length > 0) {
    const current = queue.shift();
    const children = data._edgeLineage[current] || [];
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

function drawHulls(hullG, hyperedges, nodes, birthColorFn) {
  hullG.selectAll('path').remove();
  hyperedges.filter(h => h.nodes.length > 2).forEach((h, i) => {
    const pts = h.nodes.map(nid => {
      const n = nodes.find(x => x.id === nid);
      return n && n.x != null ? [n.x, n.y] : null;
    }).filter(Boolean);
    if (pts.length >= 3) {
      const hull = d3.polygonHull(pts);
      if (hull) {
        hullG.append('path')
          .attr('d', 'M' + hull.join('L') + 'Z')
          .attr('fill', birthColorFn ? birthColorFn(h.id) : getPalette()[i % getPalette().length])
          .attr('fill-opacity', 0.06)
          .attr('stroke', birthColorFn ? birthColorFn(h.id) : getPalette()[i % getPalette().length])
          .attr('stroke-opacity', 0.15)
          .attr('stroke-width', 1);
      }
    }
  });
}

// =========================================================================
// CAUSAL GRAPH
// =========================================================================
function renderCausal() {
  const data = DATA[activeRule];
  if (!data || !data.causal_edges) return;
  const mw = MULTIWAY[activeRule];

  const svg = d3.select('#causal-svg');
  svg.selectAll('*').remove();
  const rect = document.getElementById('canvas-area').getBoundingClientRect();
  const width = rect.width, height = rect.height;
  svg.attr('width', width).attr('height', height);

  // === Build multiway causal graph if available ===
  let allEvNodes = [];   // {id, step, consumed, produced, onPath}
  let allCausalEdges = []; // {source, target}
  let eventInfo = {};

  // Greedy path events (on-path = true) + skipped alternative matches (on-path = false).
  // At each step, we replay matches using the JS engine to find ALL possible matches,
  // then mark the greedy-selected ones as on-path and the rest as alternatives.
  const greedyEvents = (data.events || []).flat();
  let greedyStep = 0;
  for (const stepEvents of (data.events || [])) {
    for (const ev of stepEvents) {
      allEvNodes.push({ id: ev.id, step: greedyStep, consumed: ev.consumed, produced: ev.produced, onPath: true });
      eventInfo[ev.id] = ev;
    }
    greedyStep++;
  }
  for (const [a, b] of (data.causal_edges || [])) {
    allCausalEdges.push({ source: a, target: b });
  }

  // Now find alternative (skipped) matches at each step using the JS engine
  const ruleDef = RULES.find(r => r.id === activeRule);
  let parsedRule = null;
  if (ruleDef) parsedRule = parseNotation(ruleDef.notation);
  else if (data._customRule) parsedRule = data._customRule;

  if (parsedRule && data.states) {
    let altId = 100000;
    const maxAltStep = Math.min((data.states || []).length - 1, currentStep);

    for (let s = 0; s < maxAltStep; s++) {
      const state = data.states[s];
      if (!state) continue;
      const matches = HGEngine.findMatches(state, parsedRule.lhs);

      // The greedy events at this step consumed certain edge indices.
      // Any match that overlaps with a greedy match but wasn't selected is an "alternative".
      const greedyConsumedSigs = new Set();
      const stepEvts = (data.events[s] || []);
      for (const ev of stepEvts) {
        greedyConsumedSigs.add(JSON.stringify(ev.consumed));
      }

      for (const [matchedIndices, binding] of matches) {
        const consumed = matchedIndices.map(i => state[i]);
        const sig = JSON.stringify(consumed);
        if (greedyConsumedSigs.has(sig)) continue; // already a greedy event

        const node = { id: altId, step: s, consumed, produced: [], onPath: false };
        allEvNodes.push(node);
        eventInfo[altId] = node;

        // Causal: this alt event consumed edges produced by earlier events
        const cSet = new Set(consumed.map(e => JSON.stringify(e)));
        for (const prev of allEvNodes) {
          if (prev.id === altId || prev.step >= s) continue;
          if ((prev.produced || []).some(e => cSet.has(JSON.stringify(e)))) {
            allCausalEdges.push({ source: prev.id, target: altId });
          }
        }
        // Also connect to greedy events at same step that share consumed edges (conflict)
        for (const gev of stepEvts) {
          const gConsumed = new Set((gev.consumed || []).map(e => JSON.stringify(e)));
          if (consumed.some(e => gConsumed.has(JSON.stringify(e)))) {
            // They overlap — show as sibling (no causal edge, just proximity)
          }
        }

        altId++;
      }
    }
  }

  // Filter to events < currentStep (events at step S produce state S+1)
  const visibleEvents = allEvNodes.filter(e => e.step < currentStep);
  const visibleIds = new Set(visibleEvents.map(e => e.id));
  const pathEventIds = new Set(visibleEvents.filter(e => e.onPath).map(e => e.id));

  const filteredEdges = allCausalEdges.filter(e => visibleIds.has(e.source) && visibleIds.has(e.target));

  // Event step map + layout
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
    .attr('fill', d => {
      if (pathEventIds.has(d.id)) return '#ff4444';
      return isDark ? '#4cdd8a' : '#1a9f5c';
    })
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

  const sim = d3.forceSimulation(nodes)
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

  container.innerHTML = `
    <div style="max-width: 800px; margin: 0 auto;">
      <h2 style="font-size: 18px; font-weight: 600; color: var(--text-heading); margin-bottom: 4px;">${rule.name}</h2>
      <p style="font-family: JetBrains Mono, monospace; font-size: 12px; color: var(--accent); margin-bottom: 24px;">${rule.notation}</p>
      <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 24px;">
        <div style="background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 16px;">
          <h4 style="font-size: 10px; text-transform: uppercase; letter-spacing: 1px; color: var(--text-dim); margin-bottom: 12px;">Node & Edge Growth</h4>
          <canvas id="chart-growth" width="340" height="180"></canvas>
        </div>
        <div style="background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 16px;">
          <h4 style="font-size: 10px; text-transform: uppercase; letter-spacing: 1px; color: var(--text-dim); margin-bottom: 12px;">Dimension Estimate</h4>
          <canvas id="chart-dim" width="340" height="180"></canvas>
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

  drawLineChart('chart-growth', stats.filter(s => s.num_nodes !== undefined), [
    { key: 'num_nodes', color: '#6c7bff', label: 'Nodes' },
    { key: 'num_edges', color: '#ff6b9d', label: 'Edges' }
  ]);
  const dimStats = stats.filter(s => s.estimated_dimension != null);
  if (dimStats.length > 0) {
    drawLineChart('chart-dim', dimStats, [
      { key: 'estimated_dimension', color: '#4cdd8a', label: 'Dimension' }
    ], true);
  }
}

function drawLineChart(canvasId, data, series, fixedRange) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const W = canvas.width, H = canvas.height;
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
    }, 1200);
  } else {
    btn.innerHTML = '&#9654;';
    clearInterval(playTimer);
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
  if (opt === 'nudge') {
    document.getElementById('nudge-hint').hidden = !opts.nudge;
    // Re-render to attach/detach nudge handlers
  }
  renderCurrentView();
}

function showTooltip(event, text) {
  const tip = document.getElementById('tooltip');
  tip.innerHTML = text.replace(/\n/g, '<br>');
  tip.style.display = 'block';
  tip.style.left = (event.pageX + 12) + 'px';
  tip.style.top = (event.pageY - 8) + 'px';
}
function hideTooltip() { document.getElementById('tooltip').style.display = 'none'; }

document.addEventListener('keydown', e => {
  const data = DATA[activeRule];
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

// State: arrays of arrays of variable names
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

    // Add/remove element buttons
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

    // Remove edge button
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
  const opts = side === 'init' ? INIT_VALS : VARS;
  map[side].push([opts[0], opts[1]]);
  renderEditorEdges(side, map[side], opts);
  updatePreview();
}

function updatePreview() {
  const fmt = edges => '{' + edges.map(e => '{' + e.join(',') + '}').join(',') + '}';
  const preview = fmt(editorLHS) + ' \u2192 ' + fmt(editorRHS);
  document.getElementById('custom-preview').textContent = preview;
}

// =========================================================================
// IN-BROWSER HYPERGRAPH ENGINE
// =========================================================================
const HGEngine = (() => {
  let _nextId = 0;
  function fresh() { return ++_nextId; }
  function reset(start) { _nextId = start; }

  function findMatches(hyp, pattern) {
    const results = [];
    _matchRec(hyp, pattern, 0, [], {}, new Set(), results);
    // Deduplicate matches with identical edge indices + bindings
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
      // Try all permutations of the hyperedge (undirected matching)
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

  function ruleNewVars(lhs, rhs) {
    const lv = new Set(); lhs.forEach(e => e.forEach(v => lv.add(v)));
    const nv = new Set(); rhs.forEach(e => e.forEach(v => { if (!lv.has(v)) nv.add(v); }));
    return nv;
  }

  function applyAllNonOverlapping(hyp, lhs, rhs) {
    const matches = findMatches(hyp, lhs);
    const used = new Set();
    const selected = [];
    for (const [mi, bind] of matches) {
      if (mi.some(i => used.has(i))) continue;
      selected.push([mi, bind]); mi.forEach(i => used.add(i));
    }
    if (!selected.length) return [hyp, []];

    const newVars = ruleNewVars(lhs, rhs);
    const allMatched = new Set(); selected.forEach(([mi]) => mi.forEach(i => allMatched.add(i)));
    const remaining = hyp.filter((_, i) => !allMatched.has(i));
    const events = [];
    const newEdges = [];

    for (const [mi, bind] of selected) {
      for (const v of newVars) bind[v] = fresh();
      const produced = rhs.map(re => re.map(v => bind[v]));
      newEdges.push(...produced);
      events.push({
        consumed: mi.map(i => hyp[i]),
        produced: produced,
      });
    }
    return [remaining.concat(newEdges), events];
  }

  function evolve(initHyp, lhs, rhs, steps) {
    // Find max existing node id
    let maxN = 0;
    for (const e of initHyp) for (const n of e) if (typeof n === 'number' && n > maxN) maxN = n;
    reset(maxN);

    const states = [initHyp.map(e => e.slice())];
    const allEvents = [];
    let evId = 0;
    let current = initHyp;
    const causalEdges = [];
    const flatEvents = [];

    for (let s = 0; s < steps; s++) {
      const [next, stepEvts] = applyAllNonOverlapping(current, lhs, rhs);
      stepEvts.forEach(ev => { ev.id = evId++; });

      // Causal: event depends on prev event if it consumed something prev produced
      for (const ev of stepEvts) {
        const cSet = new Set(ev.consumed.map(e => JSON.stringify(e)));
        for (const prev of flatEvents) {
          if (prev.produced.some(e => cSet.has(JSON.stringify(e)))) {
            causalEdges.push([prev.id, ev.id]);
          }
        }
      }

      flatEvents.push(...stepEvts);
      allEvents.push(stepEvts.map(e => ({id: e.id, consumed: e.consumed, produced: e.produced})));
      states.push(next.map(e => e.slice()));

      // Stats
      current = next;
    }

    // Build stats
    const stats = states.map((st, i) => {
      const nodes = new Set(); st.forEach(e => e.forEach(n => nodes.add(n)));
      return { step: i, num_nodes: nodes.size, num_edges: st.length, estimated_dimension: null };
    });

    return { states, events: allEvents, causal_edges: causalEdges, stats, multiway_states: {}, multiway_edges: [] };
  }

  function applyRuleOnce(hyp, lhs, rhs, matchIdx) {
    const matches = findMatches(hyp, lhs);
    if (matchIdx >= matches.length) return null;
    const [mi, binding] = matches[matchIdx];
    const newVars = ruleNewVars(lhs, rhs);
    for (const v of newVars) binding[v] = fresh();
    const matched = new Set(mi);
    const remaining = hyp.filter((_, i) => !matched.has(i));
    const produced = rhs.map(re => re.map(v => binding[v]));
    return {
      state: remaining.concat(produced),
      event: { consumed: mi.map(i => hyp[i]), produced }
    };
  }

  function stateHash(hyp) {
    return JSON.stringify(hyp.map(e => e.slice().sort((a,b) => a-b)).sort((a,b) => JSON.stringify(a) < JSON.stringify(b) ? -1 : 1));
  }

  // Isomorphism-invariant canonical hash via color refinement (2-3 rounds).
  // Handles self-loops, directed hyperedges, multi-edges.
  function canonicalHash(hyp) {
    if (!hyp.length) return '[]';
    const nodeSet = new Set();
    for (const e of hyp) for (const n of e) nodeSet.add(n);
    const nodes = Array.from(nodeSet);
    const N = nodes.length, E = hyp.length;

    // Color refinement until stable (max 10 rounds)
    function refine(color0) {
      const c = {};
      for (const n of nodes) c[n] = color0[n];
      for (let round = 0; round < 10; round++) {
        const sigs = {};
        for (const n of nodes) {
          const es = [];
          for (const e of hyp) {
            const cnt = e.filter(x => x === n).length;
            if (!cnt) continue;
            es.push(cnt + '/' + e.length + ':' + e.map(x => c[x]).sort((a,b)=>a-b).join(','));
          }
          es.sort();
          sigs[n] = c[n] + '|' + es.join(';');
        }
        const uniq = [...new Set(Object.values(sigs))].sort();
        const idx = {}; uniq.forEach((v,i) => idx[v] = i);
        let changed = false;
        for (const n of nodes) {
          const nv = idx[sigs[n]];
          if (nv !== c[n]) changed = true;
          c[n] = nv;
        }
        if (!changed) break;
      }
      return c;
    }

    // Compute sorted edge-list string for a given relabeling (undirected: sort within each edge)
    function edgeStr(relabel) {
      return JSON.stringify(
        hyp.map(e => e.map(n => relabel[n]).sort((a,b) => a-b))
          .sort((a,b) => {
            for (let i = 0; i < Math.max(a.length, b.length); i++) {
              if ((a[i]||0) !== (b[i]||0)) return (a[i]||0) - (b[i]||0);
            }
            return a.length - b.length;
          })
      );
    }

    // Initial color = degree
    const initC = {};
    for (const n of nodes) initC[n] = 0;
    for (const e of hyp) for (const n of e) initC[n]++;
    const color = refine(initC);

    // Group nodes by color class
    const cells = {};
    for (const n of nodes) {
      const c = color[n];
      if (!cells[c]) cells[c] = [];
      cells[c].push(n);
    }
    const colorOrder = Object.keys(cells).map(Number).sort((a,b)=>a-b);

    // Count total permutations needed
    let totalPerms = 1;
    for (const c of colorOrder) {
      let f = 1;
      for (let i = 2; i <= cells[c].length; i++) f *= i;
      totalPerms *= f;
      if (totalPerms > 5000) break;
    }

    if (totalPerms <= 5000) {
      // Brute-force: try all permutations within each color class,
      // take lex-smallest canonical edge list
      let best = null;
      const cellArrays = colorOrder.map(c => cells[c]);

      function permute(arr) {
        if (arr.length <= 1) return [arr.slice()];
        const result = [];
        for (let i = 0; i < arr.length; i++) {
          const rest = arr.slice(0,i).concat(arr.slice(i+1));
          for (const p of permute(rest)) {
            result.push([arr[i], ...p]);
          }
        }
        return result;
      }

      function tryCells(ci, relabel, nextLbl) {
        if (ci >= cellArrays.length) {
          const s = edgeStr(relabel);
          if (!best || s < best) best = s;
          return;
        }
        const cellPerms = permute(cellArrays[ci]);
        for (const perm of cellPerms) {
          const r = Object.assign({}, relabel);
          let lbl = nextLbl;
          for (const n of perm) r[n] = lbl++;
          tryCells(ci + 1, r, lbl);
        }
      }

      tryCells(0, {}, 0);
      return N + ':' + E + ':' + best;
    }

    // Fallback for large graphs: individualization-refinement with timeout
    const t0 = Date.now();

    function solve(col, assigned, nextLbl) {
      if (Date.now() - t0 > 500) {
        // Timeout: assign remaining by color then arbitrary order
        const r = Object.assign({}, assigned);
        let lbl = nextLbl;
        const rem = nodes.filter(n => r[n] === undefined)
          .sort((a,b) => col[a] !== col[b] ? col[a]-col[b] : a-b);
        for (const n of rem) r[n] = lbl++;
        return edgeStr(r);
      }
      const unassigned = nodes.filter(n => assigned[n] === undefined);
      if (!unassigned.length) return edgeStr(assigned);

      // Group unassigned by color, assign singletons, find first ambiguous
      const byC = {};
      for (const n of unassigned) {
        const c = col[n];
        if (!byC[c]) byC[c] = [];
        byC[c].push(n);
      }
      const cKeys = Object.keys(byC).map(Number).sort((a,b)=>a-b);
      const r = Object.assign({}, assigned);
      let lbl = nextLbl;
      let ambig = null;
      for (const c of cKeys) {
        if (byC[c].length === 1) {
          r[byC[c][0]] = lbl++;
        } else {
          ambig = { color: c, nodes: byC[c] };
          break;
        }
      }
      if (!ambig) return edgeStr(r);

      // Try individualizing each node in the ambiguous cell
      let best = null;
      for (const n of ambig.nodes) {
        const nr = Object.assign({}, r);
        nr[n] = lbl;
        const nc = {};
        for (const m of nodes) nc[m] = col[m];
        nc[n] = Math.max(...nodes.map(m => col[m])) + 1;
        const refined = refine(nc);
        const candidate = solve(refined, nr, lbl + 1);
        if (!best || candidate < best) best = candidate;
      }
      return best;
    }

    const canonical = solve(color, {}, 0);
    return N + ':' + E + ':' + canonical;
  }

  return { evolve, findMatches, applyRuleOnce, ruleNewVars, stateHash, canonicalHash, reset: reset };
})();

// =========================================================================
// MULTIWAY COMPUTATION
// =========================================================================
function computeMultiway(ruleId) {
  const data = DATA[ruleId];
  if (!data) return;

  // Parse rule from RULES or custom
  const ruleDef = RULES.find(r => r.id === ruleId);
  let lhs, rhs;
  if (ruleDef) {
    // Parse notation
    const parsed = parseNotation(ruleDef.notation);
    if (!parsed) return;
    lhs = parsed.lhs; rhs = parsed.rhs;
  } else if (data._customRule) {
    lhs = data._customRule.lhs; rhs = data._customRule.rhs;
  } else return;

  const initState = data.states[0];

  // Reset node counter
  let maxN = 0;
  for (const e of initState) for (const n of e) if (typeof n === 'number' && n > maxN) maxN = n;
  HGEngine.reset(maxN);

  const initHash = HGEngine.canonicalHash(initState);
  const mwStates = {}; // hash -> {state, step}
  const mwEdges = [];  // {from, to, event, fromHash, toHash}
  mwStates[initHash] = { state: initState, step: 0 };

  let branches = { [initHash]: initState };
  const MAX_STATES = 300;
  const MAX_STEPS = 4;
  const MAX_TIME = 3000; // 3 seconds max
  const startTime = Date.now();
  let eventId = 0;

  for (let step = 1; step <= MAX_STEPS; step++) {
    const newBranches = {};
    let tooMany = false;

    for (const [parentHash, parentState] of Object.entries(branches)) {
      const savedId = eventId; // save for branching
      const matches = HGEngine.findMatches(parentState, lhs);
      if (!matches.length) {
        newBranches[parentHash] = parentState;
        continue;
      }

      for (let mi = 0; mi < matches.length; mi++) {
        if (Object.keys(mwStates).length >= MAX_STATES || Date.now() - startTime > MAX_TIME) { tooMany = true; break; }

        const result = HGEngine.applyRuleOnce(parentState.map(e=>e.slice()), lhs, rhs, mi);
        if (!result) continue;
        const childHash = HGEngine.canonicalHash(result.state);
        if (!mwStates[childHash]) {
          mwStates[childHash] = { state: result.state, step };
        }
        newBranches[childHash] = result.state;
        const ev = { id: eventId++, consumed: result.event.consumed, produced: result.event.produced };
        mwEdges.push({ from: parentHash, to: childHash, event: ev });
      }
      if (tooMany) break;
    }
    branches = newBranches;
    if (tooMany) break;
  }

  // Build the default path: follow first child at each step through the multiway DAG
  const defaultPathHashes = new Set([initHash]);
  const defaultPathEventIds = new Set();
  let currentHash = initHash;
  for (let step = 1; step <= MAX_STEPS; step++) {
    const child = mwEdges.find(e => e.from === currentHash && mwStates[e.to] && mwStates[e.to].step === step);
    if (!child) break;
    defaultPathHashes.add(child.to);
    defaultPathEventIds.add(child.event.id);
    currentHash = child.to;
  }

  MULTIWAY[ruleId] = { states: mwStates, edges: mwEdges, initHash, defaultPathEventIds, defaultPathHashes };
  selectedPath = null;
  selectedMultiwayNode = null;
}

function parseNotation(notation) {
  // Parse arrow notation like "{{x,y},{x,z}} → {{x,z},{x,w},{y,w},{z,w}}"
  const arrow = notation.includes('\u2192') ? '\u2192' : '->';
  if (!notation.includes(arrow)) return null;
  const [lStr, rStr] = notation.split(arrow);
  function parseSide(s) {
    s = s.trim();
    // Strip outermost braces: {{x,y},{x,z}} -> {x,y},{x,z}
    if (s.startsWith('{{')) s = s.slice(1, -1);
    else if (s.startsWith('{')) s = s.slice(1, -1);
    const edges = [];
    for (const m of s.matchAll(/\{([^}]+)\}/g)) {
      edges.push(m[1].split(',').map(v => v.trim()));
    }
    return edges;
  }
  return { lhs: parseSide(lStr), rhs: parseSide(rStr) };
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

  if (!mw || Object.keys(mw.states).length < 2) {
    svg.append('text').attr('x', width/2).attr('y', height/2)
      .attr('text-anchor', 'middle').attr('fill', isDark ? '#666' : '#999')
      .attr('font-size', 14).text('Computing multiway system...');
    return;
  }

  const g = svg.append('g');
  svg.call(d3.zoom().scaleExtent([0.05, 20]).on('zoom', e => g.attr('transform', e.transform)));

  // Build nodes and links
  const nodeMap = {};
  const nodes = [];
  for (const [hash, info] of Object.entries(mw.states)) {
    const n = { id: hash, step: info.step, edgeCount: info.state.length };
    nodes.push(n);
    nodeMap[hash] = n;
  }

  // Build links: expand multiple transitions as separate curved arrows
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
      // curve offset: spread evenly around center, 0 for single
      const curve = count === 1 ? 0 : (i - (count - 1) / 2) * 25;
      links.push({ source: from, target: to, curve, count, idx: i });
    }
  }

  // Default path hashes for highlighting
  const defaultPathHashes = mw.defaultPathHashes || new Set([mw.initHash]);
  const defaultPathEdges = new Set();
  for (const me of mw.edges) {
    if ((mw.defaultPathEventIds || new Set()).has(me.event.id)) {
      defaultPathEdges.add(me.from + '|' + me.to);
    }
  }

  // Selected path hashes
  const selPathHashes = new Set();
  const selPathEdgeKeys = new Set();
  if (selectedMultiwayNode) {
    // Trace path from init to selected node
    const path = tracePathTo(mw, selectedMultiwayNode);
    selPathHashes.add(mw.initHash);
    for (const e of path) {
      selPathHashes.add(e.to);
      selPathEdgeKeys.add(e.from + '|' + e.to);
    }
  }

  const maxStep = Math.max(1, ...nodes.map(n => n.step));
  // Spread nodes by step (y) and index within step (x)
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

  // Arrow marker
  g.append('defs').append('marker')
    .attr('id', 'mw-arrow').attr('viewBox', '0 -3 6 6').attr('refX', 10)
    .attr('markerWidth', 5).attr('markerHeight', 5).attr('orient', 'auto')
    .append('path').attr('d', 'M0,-3L6,0L0,3').attr('fill', isDark ? '#66668880' : '#88889980');

  g.append('defs').append('marker')
    .attr('id', 'mw-arrow-red').attr('viewBox', '0 -3 6 6').attr('refX', 10)
    .attr('markerWidth', 5).attr('markerHeight', 5).attr('orient', 'auto')
    .append('path').attr('d', 'M0,-3L6,0L0,3').attr('fill', '#ff4444');

  // Links — curved paths for multiple transitions
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
    const mx = (sx + tx) / 2 + d.curve;
    const my = (sy + ty) / 2;
    return 'M' + sx + ',' + sy + 'Q' + mx + ',' + my + ' ' + tx + ',' + ty;
  }
  const link = g.append('g').selectAll('path').data(links).join('path')
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

  // Nodes
  const node_el = g.append('g').selectAll('circle').data(nodes).join('circle')
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
        `${st.state.length} edges, ${new Set(st.state.flat()).size} nodes\n` +
        (d.id === mw.initHash ? '(initial state)' : 'Click to select this branch')
      );
    })
    .on('mouseleave', hideTooltip)
    .on('click', (ev, d) => {
      ev.stopPropagation();
      if (d.id === selectedMultiwayNode) {
        // Deselect — revert to default
        selectedMultiwayNode = null;
        selectedPath = null;
      } else {
        selectedMultiwayNode = d.id;
        // Build path from init to this node
        const pathEdges = tracePathTo(mw, d.id);
        selectedPath = pathEdges.map(e => e.event);
      }
      renderMultiway();
    });

  // Step labels on left
  for (let s = 0; s <= maxStep; s++) {
    g.append('text').attr('x', 20).attr('y', 60 + s * (height - 120) / maxStep + 4)
      .attr('fill', isDark ? '#666' : '#999').attr('font-size', 11)
      .attr('font-family', 'JetBrains Mono').text(`t=${s}`);
  }

  // Legend
  const lg = g.append('g').attr('transform', `translate(${width - 180}, 20)`);
  lg.append('circle').attr('cx', 0).attr('cy', 0).attr('r', 5).attr('fill', '#ff4444');
  lg.append('text').attr('x', 10).attr('y', 4).attr('fill', isDark ? '#aaa' : '#555')
    .attr('font-size', 11).text('Default path (greedy)');
  lg.append('circle').attr('cx', 0).attr('cy', 20).attr('r', 5).attr('fill', '#44aaff');
  lg.append('text').attr('x', 10).attr('y', 24).attr('fill', isDark ? '#aaa' : '#555')
    .attr('font-size', 11).text('Selected path');

  // Info text
  g.append('text').attr('x', width/2).attr('y', height - 15)
    .attr('text-anchor', 'middle').attr('fill', isDark ? '#555' : '#999')
    .attr('font-size', 11)
    .text(`${Object.keys(mw.states).length} states, ${mw.edges.length} transitions (${Math.min(4, maxStep)} steps)`);
}

function tracePathTo(mw, targetHash) {
  // BFS backwards from target to initHash
  const parent = {};
  const parentEdge = {};
  const queue = [mw.initHash];
  const visited = new Set([mw.initHash]);

  // Build adjacency forward
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

  // Reconstruct path
  const path = [];
  let cur = targetHash;
  while (cur !== mw.initHash && parent[cur]) {
    path.unshift(parentEdge[cur]);
    cur = parent[cur];
  }
  return path;
}

let customRuleCounter = 0;

function runCustomRule() {
  const btn = document.querySelector('.run-btn');
  const errEl = document.getElementById('custom-error');
  errEl.style.display = 'none';

  // Show computing state
  btn.disabled = true;
  btn.textContent = 'Computing\u2026';
  btn.style.opacity = '0.7';

  // Use setTimeout so the UI updates before the heavy computation
  setTimeout(() => {
    try {
      const steps = Math.min(15, Math.max(1, +document.getElementById('custom-steps').value || 8));
      const lhs = editorLHS.map(e => e.slice());
      const rhs = editorRHS.map(e => e.slice());
      const init = editorInit.map(e => e.map(v => parseInt(v)));

      if (lhs.length === 0 || rhs.length === 0 || init.length === 0) {
        throw new Error('Need at least 1 edge in LHS, RHS, and init');
      }

      const result = HGEngine.evolve(init, lhs, rhs, steps);

      // Unique ID for this custom rule
      customRuleCounter++;
      const ruleId = 'custom_' + customRuleCounter;
      DATA[ruleId] = result;
      DATA[ruleId]._customRule = { lhs, rhs };
      buildLineageMaps();

      const fmt = edges => '{' + edges.map(e => '{' + e.join(',') + '}').join(',') + '}';
      const notation = fmt(lhs) + ' \u2192 ' + fmt(rhs);
      const finalNodes = result.stats[result.stats.length-1].num_nodes;
      RULES.push({
        id: ruleId,
        name: 'Custom #' + customRuleCounter,
        notation: notation,
        desc: finalNodes + ' nodes at step ' + steps,
        tag: 'Custom', tagClass: 'tag-custom',
        blurb: 'Custom rule: ' + notation + '. Starting from ' + fmt(editorInit) + ', evolved for ' + steps + ' steps. Final graph: ' + finalNodes + ' nodes, ' + result.stats[result.stats.length-1].num_edges + ' edges.',
        isCustom: true,
      });

      renderRuleCards();
      selectRule(ruleId);

      // Show ready state
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
  }, 50);
}

// Initialize editor on load
setTimeout(initEditor, 100);
</script>
<script>
window.__HYPERGRAPH_DATA__ = ''' + data_str + ''';
init();
</script>
</body>
</html>'''

out_path = os.path.join(tmp, 'index.html')
with open(out_path, 'w', encoding='utf-8') as f:
    f.write(HTML)

print(f'Written: {len(HTML)//1024}KB to {out_path}')
