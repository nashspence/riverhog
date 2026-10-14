# Integration-agent handoff: generic asynchronous storage

Read the [2026-10-14 decision](https://github.com/nashspence/riverhog/issues/949#issuecomment-6172547034) alongside the
[2026-10-04 ownership/effect decisions](https://github.com/nashspence/riverhog/issues/949#issuecomment-5981858530).
The newer payload/backpressure clarification controls any conflicting earlier
staging language. Earlier conversation is intent calibration, not blanket approval.

**Primary technical guide: [EXTENT_AND_BACKPRESSURE.md](EXTENT_AND_BACKPRESSURE.md).**
It records the zero-spool boundary, separate payload/control-state budgets,
per-step and total-growth obligations, transport research, current-source gap,
constructive witnesses and remaining real-validation work.

## Integration boundaries

Riverhog coordinates opaque objects and generic external effects. It does not
inventory media, interpret mount paths, select physical topology, instruct operators,
or depend on Home Assistant. Adapters/deployments own those concerns.

Known sink waiting stops applicable upload body production/consumption. An exact
pending identity is not permission to accept data into a backlog. No Riverhog
file/database/RAM/cache spool is added. Existing cache behavior is not a producer
upload fallback, and no new copy-staging mechanism is selected.

Integrate admission through maintained producer, upload HTTP ingress, service/ports,
adapter effects and response validation. Gate before receive/allocation, preserve
bounded race/in-flight handling, and reconcile exact receipts before asking for
payload again. Keep shared coordination responsive. Required real integration
tests must cover all those boundaries, not only the two new reference models.

## Retained ownership evidence

The original semantic/lease models and their 77 tests remain unchanged. They
exercise exact consumer/incarnation/object identities, finite renewable ownership,
lost replies, release-before-prepare, terminal-parent cleanup and active-stream pins.

Expiry/release closes new admission but is not worker-cessation or provider-cleanup
evidence. Persist debt before effects; retain it until authoritative settlement.
A renewed grant must not append an unbounded renewal history. Bounded replay
retention must still reject delayed retired identities. No timer alone proves
quiescence, and finite read ownership does not authorize expiry of durable writes.

The prefix-window/ordinal construction remains ONE witness, not the chosen API.
It assumes an authenticated finite scope domain, durable allocation/clock authority,
non-reused identities and real worker fencing. Low ordinals/gaps can cause
backpressure; its head-of-line and scope-growth costs need integration decisions.
The earlier unbounded dictionaries/event lists and resource-wide scans are semantic
fixtures, not production extent/growth contracts.

## Evidence and remaining validation

`source_audit_v3.json` audits 19 selected files at
`e0036f80410a83cc5eaa9d63c17650ffae72e2bd`; 12 match the reference parent's blobs,
7 changed. Earlier source-equivalence claims must not be generalized to current main.

`validation_v3.json` records 123 passing reference tests, counted transfer growth,
indexed SQLite selection and a source-function characterization that confirms
body-before-storage-call behavior in audited main. The negative source result
is not an application-gate acceptance pass. Actual product package collection was
blocked by missing `rfc8785`; full product/HTTP/proxy/provider/production concurrency
and growth validation remain outstanding.

Choose the smallest coherent production contract with its owning schema/config/
operation declarations and tests. Do not add a parallel production extent registry.
Satisfy #949's issue-owned acceptance, current repository validation and actual
integration-SHA checks before treating the feature as complete.

## Reference provenance

This append-only revision builds on `705cd8c79e48c190fedece4105f2f6e9345994c7`;
no main merge, rebase or force-push is intended. The underlying mapped source base
remains `6f4099700834e411d7b7b342c43c077fcc83e55e`.
The original `78415d14ae5e5556a4834267cf93053bb6af66f3` handoff and #950 mapping
remain historical provenance. The new exact SHA recorded in #949 is the preferred
reference input, not integration authority. Previous docs remain in branch history.
