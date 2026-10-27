# Validation evidence and limits

## Exact relationship

Audited implementation / experiment fixture:
`f41bae6e1456d4ae16952de293320cbe4a6ce460`.
Additional inspected integration tip:
`93a3208056a98887682db0f5c16f6e868d5dc141` (two helper renames only).
Prior reference: `cd2a10e8972ed79a2641faf2bd68f1d4c2036000`.
Only `.reference/` material is added. Product source remains identical to the base.

## Actually performed

The command `python .reference/probe.py` ran ten reference tests successfully.
[PROBE_RESULT.json](PROBE_RESULT.json) contains the runtime identity, timing and
actual results. It uses scratch Linux/Python 3.13.5/SQLite 3.46.1, not the locked
product or native Windows runtime. Source ID equality was checked between the
loaded stdlib engine and the VFS API before registration.

The probe imports the audited repository's test-only VFS fixture. It checks the
reference arithmetic and rejection bookkeeping, then performs real SQLite
transactions with a limited xWrite gate. It demonstrated:

- A successful 256 KiB fixed-control mutation inside the derived envelope.
- Refusal of the third distinct DB page write, automatic rollback of 66 original
  pages under a separate bank, original bytes preserved and integrity_check OK.
- Journal-extent refusal before any database write, with original bytes intact.
- Structural SQL rejection under a three-page quota, not a false success bound.
- Aggregate SQLite hard-heap refusal in an isolated probe process; original SQL
  data remains available. No Native or total-process-memory claim follows.

The gate experiment is **not** a full production VFS: bootstrap happens before its
low test quota; it polices xWrite only, uses Python callbacks/dynamic bookkeeping,
and does not cover physical-reserve allocation, every file-control method, Windows
owner handles, cold machine reset, installed native packaging or authenticated
custody. Those are explicit integration vectors, not implied passes. No production
code or requirement is imported from the reference probe.

Static checks parsed 10 source modules and both reference Python files, resolved
25 named source symbols, verified four test locations and eight relative Markdown
links, and checked newline/fence balance and the result JSON. The first static
pass caught an incorrect Windows test filename; it was corrected before the
passing check. Reference-only changed paths and whitespace checks passed.
Published parent/tree/blob/ref identity and actual check status are checked after
publication and recorded in the owning issue.

## Not performed / not claimed

No maintained backend repair, native Windows test, locked-product suite, growth
qualification, full-volume test, machine-reset test or final-source/tested-merge
CI ran for this reference. The earlier Native141 timings and page/journal samples
belong to the agent's issue evidence, not this run.

The scratch environment has no `mise` or Docker. `make lint` was attempted and
stopped before lint execution: make exit 2 / prerequisite exit 127, with the
Makefile reporting missing mise. Unlocked product tools were not substituted. The reference checks do not qualify the proposed compiled native gate or
establish that any particular configured quota admits every structural state.

The branch is design input under #903, not a green implementation handoff. Missing,
skipped, pending or absent CI contexts are not passes. Qualification remains with
the exact integrated implementation and existing repository rail.
