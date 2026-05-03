# Causal Graph Branch Coverage and Coloring Semantics

Task: `t-2026-05-03-bf09dad3`
Author: Ada / p1
Baseline: `origin/main` at `ad493e8`

## Conclusion

This is **not** a backend/API data bug in the multiway-causal endpoint. The backend already returns branch-aware multiway-causal data with off-path events, and the `renderMultiwayCausal()` view already colors default-path vs off-path events red/green.

The reported "all red" symptom is a **semantic/product mismatch** in the ordinary `renderCausal()` view: it intentionally renders only the realized greedy history from `data.events` + `data.causal_edges`, and it colors every node/edge red. That view is not multiway/branch-aware.

## Evidence

1. `client/app.js::renderCausal()` renders only server-provided causal events and edges, with every node and edge hard-coded red.
2. `client/app.js::renderMultiwayCausal()` uses `data.default_path_event_ids` to render the greedy path in red and off-path events/edges in green.
3. Current multiway-causal payloads contain substantial off-path structure on branching rules:
   - rule1: 4999 events, default path length 4, off-path 4995.
   - rule4: 2696 events, default path length 3, off-path 2693.
   This confirms the green branch structure is already present in the API data.

## Reproduction Notes

- Multiway-causal API on branching rules is branch-aware and nontrivial.
- The ordinary causal graph is single-history only and stays all red by design.
- The multiway-causal view already carries the expected red/green semantics.

## Recommendation

Do **not** invent a fake multi-history causal model inside `renderCausal()`.

If the product goal is for users to see possible-history structure with green branches, the next change should be a **contract/UI wording update** that either:

1. steers users to the Multiway Causal view for branch coverage, or
2. explicitly redesigns the ordinary causal view to show an opt-in branch overlay with clear labeling.

The backend/API already supports the branch-aware multiway-causal representation required for the green structure.
