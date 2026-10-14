# Generic asynchronous-storage reference for #949

Non-authoritative executable input under [#903](https://github.com/nashspence/riverhog/issues/903).
The [latest issue decision](https://github.com/nashspence/riverhog/issues/949#issuecomment-6172547034) owns direction and acceptance.
Earlier conversation was intent calibration; fixture algorithms are not a v1 API.

Start with [the integration handoff](INTEGRATION.md) and
[the extent/backpressure guide](EXTENT_AND_BACKPRESSURE.md).

Riverhog must not spool producer payload for an unavailable destination. Known waits
propagate upstream; only bounded transient in-flight buffers and admitted metadata
are permitted. No removable-media domain, topology API or implicit cache fallback
is introduced. Existing explicitly selected cache/custody contracts keep their roles.

## Contents and reproduction

- `semantic_model.py`, `lease_model.py` and their tests retain the original 77
  semantic/finite-ownership witnesses unchanged.
- `backpressure_model.py` and its 23 tests add lazy source admission, effect-boundary
  races, receipt reconciliation and counted linear transfer work.
- `work_budget_model.py` and its 23 tests add bounded SQLite metadata, capacity/debt,
  indexed scheduling, retirement, reopen and churn witnesses.
- `audit_body_gate.py` characterizes the exact audited upload function with doubles.
- `source_audit_v3.json` and `validation_v3.json` record this revision's source identity
  and measured evidence; `source_audit.json` retains the preceding audit.

```sh
python3 -m unittest discover -s .reference/949 -p 'test_*.py' -v
python3 -m compileall -q .reference/949
python3 .reference/949/audit_body_gate.py --repo /path/to/checkout-with-audited-commit
```

123 reference tests passed on Python 3.13.5. These are not full product, live HTTP,
provider or distributed-crash proofs. See the guides and `.reference/HANDOFF.md`
for assumptions, actual timing, blocked validation and publication provenance.

Current source audit: `e0036f80410a83cc5eaa9d63c17650ffae72e2bd`.
Reference parent: `705cd8c79e48c190fedece4105f2f6e9345994c7`.
Only `.reference/` changes. Implement on then-current authoritative main, not this
older production checkout; do not merge the reference mechanically.
