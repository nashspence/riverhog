# Focused receiving and reproduction

Reconcile the new numeric-evaluation delta after the accepted lookup-map slice against the receiving source. The baseline reader and lookup tests are retained exactly from `0e9bef9445213aadadb7218fc6e268cc324f35f3`; the audited integration base is `824383a8a8143aac033f86dd1562a5979f6ec4da`. Preserve the receiver's unrelated work and its existing reader budgets.

Use the repository's locked Python/uv environment and an exact clean source. The producer pins are Python 3.12.3, uv 0.11.24, pytest 9.1.1, Ruff 0.16.0, REUSE 6.2.0, and mypy 2.3.0. Temporary directories, pytest basetemp, receipts, and caches belong outside the checkout.

## Focused owner verification

The frozen verification driver is `evidence/verify.py.txt`. Copy it to a new external Python file, then run it with the checkout's `.venv/bin/python`, pinned uv on PATH, `--repo`, `--expected-sha`, and a new external `--out`.

The maintained pytest timing plugin receives only the new numeric-evaluation file and unchanged growth-declaration/bound owner tests. The driver then runs repository Ruff check, formatting, REUSE, and the maintained mypy entrypoint sequentially through the maintained child/timing wrappers. It checks exact source/tool pins, retains original exits/logs/controller records, and removes only its owned external temporary tree. This is a focused owner selection; it does not rerun the broad unit suite or actual caller fixtures.

## Retained replay

Restore the exact projection and sample bytes from `evidence/retained-inputs.json` and their gzip/base64 archives outside the checkout. Verify both lengths and hashes before use. Preserve the complete 46-group transcript in `evidence/prior-replay-run.json`, including every independent root occurrence and overlapping descendant sample.

The original inputs are not new product executions. The projection has 100 declarations. The 46 complete reachable subprojections contain 829 declaration occurrences in total; the supplied samples have 122 independent roots and 1,078 total sample nodes across roots and descendants. Each public invocation must parse and validate its projection afresh.

The frozen numeric comparison script, exact arguments, source identities, and original receipts are supplied with the completed results. It uses separate diagnostic copies and unmodified modules, one pass per reader in each category, with baseline before proposed. Complete public calls include parsing, context/index construction, unit checks, numeric checks, declaration digests, actual delegate records, and composition. Input decoding/copying, adversarial comparisons, diagnostic preparation, equality checks, and report writing remain outside unmodified elapsed intervals.

Construction counts describe metadata work. Live observation checks, required expression evaluations, fresh parses, canonical work, sample checks, and actual delegate obligations must remain equal. Do not substitute profiler timings for unmodified elapsed observations, sum nested timings as additive costs, or treat cumulative entries as live memory. No timing acceptance threshold is introduced.
