# G26 integration reference

Read [DESIGN.md](DESIGN.md) first. It selects source-normal assertion intervals,
shared exact-scope relations, compact generation commitments and nonexpanding
query paths. [RESEARCH.md](RESEARCH.md) records primary sources and alternatives.

The required owner decision is explicit: approve the proposed derived-index
extraction/digest and durable-state transition before changing that protected
boundary. Archive/member/journal/history identities and query semantics stay intact;
old index generation IDs do not. This is a recommended design, not a unilateral
approval or a completed production implementation.

Executable payload:
- [factored_index.py](factored_index.py): small SQLite factor/guard/hash/query model.
- [native_compiler.py](native_compiler.py): differential adapter for actual native
  journals/history bindings. Its Python/per-prefix views are test conveniences,
  NOT production bounded-memory or source-normal extraction code.
- [test_factored_index.py](test_factored_index.py) and
  [test_native_compiler.py](test_native_compiler.py): native/prototype witnesses.
- [profile_g26.py](profile_g26.py), [counters.json](counters.json) and
  [evidence.json](evidence.json): reproducible counters, environment and limitations.

Use a separate checkout of audited source
`152d095038df0c0d0ed642cf265d47b11c65a754`, not this reference's older runtime base.
With the repository's locked environment and all source packages available:

```sh
PYTHONPATH=/path/to/reference/.reference/g26:$PYTHONPATH \
  python -m pytest -q /path/to/reference/.reference/g26/test_factored_index.py \
  /path/to/reference/.reference/g26/test_native_compiler.py
PYTHONPATH=/path/to/reference/.reference/g26:$PYTHONPATH \
  python /path/to/reference/.reference/g26/profile_g26.py
```

The producer used a constrained native source-import environment, not the locked
workspace. Exact reproducibility/qualification details are in evidence.json.
No production migration, first-page HTTP worker, provider operation or final-image
integration is claimed. Do not copy this directory into main as a new registry or
merge the reference branch mechanically. Reconcile source and implement the mapped
owner changes from DESIGN.md through the normal integration rail.
