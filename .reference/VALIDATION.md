# Validation actually performed

Prepared October 29, 2026. Audited base `209481c189f7387557a85e1c249184642816e103`. Producer environment: Python 3.13.5 on Linux; repository pin in `mise.toml` is Python 3.12.3. Reference execution is therefore not pinned-toolchain repository qualification.

## Source acquisition and integrity

The read-only clone gateway supplied a full clone at the exact audited commit and tree. Archive SHA-256: `3d97d496a4bb6f7198cd1ebb2a65f6fad23c3632bf9fd9d38d30aa2d94407338`. Embedded manifest SHA-256: `cf3e398f19f399c73de150a7ab77d63d329c0d9260cfdc2927f8655ca8fb9466`. Both matched the supplied checksums. Executable modes lost during ZIP extraction were restored from the Git index; tracked source bytes were not edited. Network `git clone` in this environment failed; no authenticated shell or secret was used as a substitute.

## Reproducible reference check

```sh
python .reference/verify_reference.py
```

The verifier reads only two existing test files from the pinned Git commit, checks/applies the candidate patch in a temporary directory, parses all four resulting Python files, and compares the two existing ASTs after undoing only the documented watchdog plumbing. It then copies **only the newly authored helper and its newly authored tests** into a second isolated tree and runs standard-library unittest. It does not import or execute Riverhog's existing implementation/tests, conftest, dependency installation, or scripts.

Actual results:

```json
{
  "audited_base": "209481c189f7387557a85e1c249184642816e103",
  "interpreter": "3.13.5",
  "limits": "No repository imports, pytest, installed artifacts, database, browser or CI run",
  "migration_ast_equivalence": {
    "files": 2,
    "watchdog_sites": 7
  },
  "repository_tests_executed": false,
  "runs": [
    {
      "elapsed_seconds_observed": 1.119,
      "exit_status": 0,
      "scale": "1",
      "tests": 16
    },
    {
      "elapsed_seconds_observed": 0.908,
      "exit_status": 0,
      "scale": "4",
      "tests": 16
    },
    {
      "elapsed_seconds_observed": 0.992,
      "exit_status": 0,
      "scale": "0.5",
      "tests": 16
    }
  ]
}
```

Sixteen test methods passed at each factor: 48 method executions total, plus their subcases. The helper tests include one real Python-child environment propagation check per run, injected-clock deadline arithmetic, backend-range/rounding checks and an already-completed event. They do **not** exercise Riverhog's existing upload/download workers or prove how those workers respond to an induced stall. Reported elapsed values are observations, not passing thresholds.

Candidate patch SHA-256: `55390ccdf9be5d10fe48416a661c81dfffb40c3e512db62dec2394b3944d4357`. Static checks found four parseable files and seven timeout substitutions across the two existing test modules. Patch applicability and AST equivalence are limited evidence: they do not prove runtime behavior, project lint/type compliance, import policy, platform compatibility or complete timeout migration.

The read-only candidate scanner also completed against the exact base: 507 candidate files and 3,192 matching lines, with known false positives and incomplete dynamic/default discovery as described in AUDIT.md. No application code was executed to produce that scan.

## Not performed

No repository pytest run or collection, pinned Python 3.12.3 run, `make lint`, `make unit`, `make dist-smoke`, `make build`, `make linux-qualification`, PostgreSQL, Docker/Compose, Playwright browser, Windows/macOS, GPU, live provider, release/publication or GitHub Actions qualification was performed. The producer did not change any production branch, deployed configuration or release governance. Reference publication does not satisfy #983's integrated acceptance.

Startup validation, shared evidence conditions, CLI/shell/CI policy adapters, actual container propagation, semantic-witness corrections and the complete classified migration are deliberately absent from the first-slice patch. The integration owner must reconcile and validate them through the normal rail. Exact published tree/commit identity and publication verification are recorded in the owning issue's publication comment.
