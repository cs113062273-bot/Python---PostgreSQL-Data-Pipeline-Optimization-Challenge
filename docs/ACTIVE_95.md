# Active target: 95% less complete processing time

The user raised the target from 80% to 95% on 2026-10-27. The metric is time
reduction against the unchanged official original on the same controlled
workload. At the existing official median of 110.5237876s, 95% requires at most
5.52618938s. This is 20x the official speed, or a further 3.6246x versus the
qualified 20.0302624s candidate. A 120% time reduction is impossible; speed
increase and time reduction must never be confused.

The 95% target is NOT achieved. The authoritative result remains the fresh
1M three-run complete-service cohort at frozen 7b0f077: 110.5237876s official
versus 20.0302624s candidate, 81.876967% less time. The newer 17.7371s phased
transport pilot is unpaired and does not replace that repeated comparison.

New isolated experiments, all keep default code and durability unchanged:

- Typed stage: 31 native differential cases passed. Original vs typed 1M
  projection 4.759960s vs 5.243902s; stage 186130432 vs 140517376 bytes.
  Fresh complete-service typed pilot 25.8317s, exact six outputs/counters/
  six versions/schema/sequence checks passed. Rejected as a speed candidate.
- PG17 JSON_TABLE: correct TEXT/proof fingerprints on 100k, original projection
  0.487159s versus JSONB columns 3.664008s and formatted-TEXT 4.104374s.
  All launched seven workers, so the loss is not explained by missing workers.
  No full-pipeline or universal semantic claim. Not promoted.
- Index memory 512MB and up to eight maintenance workers: fresh unpaired
  complete-service pilot 18.5766s with six-table fingerprints/counters/versions/
  schema/sequence checks. Three paired rollback-only key rebuilds have medians
  2.881093s original versus 2.803867s tuned, with overlap/reversal in individual
  runs. This component gain is small and uses a larger memory budget; no new
  whole-service percentage or default promotion.
- Compact axes: new isolated prototype, 121 native semantic cases plus 48
  position/type/numeric-boundary cases passed with zero differences.
  See bench/compact_axis_probe.py. Native point coordinates encode bounded
  integer payload values exactly, not identity IDs. All input is still checked
  against the existing conservative domain, with original whole-batch fallback.
  The new native point constructor is checked by the unchanged dependency guard.
  Fresh complete-service 1M pilot was 19.165993s with exact six outputs,
  counters, six versions and original schema/sequence checks. Its initial
  proof/projection cost was 5.780714s, already over the entire 5.526189s target.
  Table2 transform/COPY cost 2.641367s. Moving computation earlier did not
  remove enough work. This is unpaired and not a new ranked percentage or a
  promoted default. Further adoption must bind the numeric encoding's bounds
  to the exact safe-domain grammar and qualify crash/resource/scale behavior.

All new 1M service runs were fresh FILE_COPY clones without target warmup.
Every new hook embeds its source identity into the actual archived batch SQL;
the native six-version provenance retains that batch SHA. Their shared
underlying compiler is still ce4f60cd7aae88e708301ccbcbe8b97dbafebded5cf8ea4cf34bd0f21028fbfd.
No new candidate has established an improvement over the repeated 81.88%
result, and none is promoted. Four evaluated directions do not prove a
hardware/algorithmic limit; 95% remains an unachieved target.

Evidence: [typed complete service](../reports/local-service-typed-stage-1m.json),
[typed component](../reports/typed-stage-component-1m.json),
[JSON_TABLE component](../reports/json-table-projection-100k.json),
[index complete service](../reports/local-service-index-tuned-1m.json),
[paired index components](../reports/index-tuning-components-1m.json),
[compact complete service](../reports/local-service-compact-axes-1m.json),
[121 semantic cases](../reports/semantics-compact-axes-121.json),
[48 boundary cases](../reports/semantics-compact-axes-boundaries.json).
The new round also passed 51 compiler tests, six unchanged organizer tests and
all 32 organizer file hashes. PG17 JSON_TABLE behavior was checked against the
[official documentation](https://www.postgresql.org/docs/17/functions-json.html#FUNCTIONS-SQLJSON-TABLE).

Prior bounded/resume work at 6949d3b remains documented in
BOUNDED_COPY_RESEARCH.md. It is a functional primitive, not the managed COPY
recovery/fencing/counter/version architecture. All original open qualification
gates in RELEASE_80_AUDIT.md still apply. Local validation first: the user has
no Linux/SSH host; do not ask again. Preserve the frozen benchmark worktrees,
original generator/seed, control history and user prompt/Downloads files.
