# Reference validation — not product qualification

Producer environment: CPython 3.13.5, Git 2.47.3, 2026-10-12.
No repository dependency installation or network service was used by these checks.
The reference uses standard-library Python; its syntax supports Python 3.10+.
Only the producer's 3.13.5 interpreter was exercised.

## Performed

1. Verified the full-clone ZIP SHA-256
   `8bd80d6d717e41dda3cc44c297aab3dc22da2744907d1babcd10953029fd79c7`
   and embedded manifest SHA-256
   `5853fefd111344dd35e7892b6cd6f5b85e0b172f5b69027edd8c82a430fc6846`.
   Git HEAD and tree match the audited identities in HANDOFF. Restored Git file
   modes after ZIP extraction and confirmed no original source-file changes.
2. Ran `PYTHONDONTWRITEBYTECODE=1 python .reference/test_claim_projection.py`:
   **22 tests passed**, including parameterized subcases inside unittest tests.
3. Parsed all three new Python files, verified local Markdown file links and
   manifest/file membership, and ran `git diff --cached --check` on the prepared
   reference additions.
4. Ran `python .reference/verify_reference.py` against the audited commit:
   reference integrity and all **17 focused source-file fingerprints matched**.
5. Exercised the verifier's failure paths with disposable local copies/objects:
   altered reference bytes were rejected (exit 1); one changed committed source
   seam was reported for reconciliation (exit 2). Neither is a product test.
6. Prepared an exact Git tree containing only the eight `.reference/` additions.
   The actual published commit/parent/tree comparison and native Development-link
   limitation are recorded in #968; no pre-publication SHA is represented as a
   published commit.

The fixture examples distinguish exact scope and attribution, passing negative
behavior, mixed purposes, missing candidate results, policy/tooling controls,
required failures, duplicate/mismatched evidence, skip/xfail/xpass, interruption,
parameter identity, and advisory metadata outside required-work/claim inputs.

## Not performed or established

No upstream project code was executed. Repository pytest collection, product
unit/integration tests, locked lint/type checks, full Linux/image/install/native
qualification, browser review, provider/release operations and hosted CI were
not run for this reference. Contract Closure was not regenerated or compared.

The example trusts already resolved owner claims and authenticated runner inputs;
its `valid` flag is a synthetic fixture value, **not artifact verification**.
It does not parse untrusted wire records, discover tests/contracts, prove fixture
completeness, validate source checkout custody, aggregate retries, or enforce all
consumer behavior. The advisory test illustrates separation of inputs; it does
not validate #963's production wrappers. No test count is product conformance.

Integration must reproduce the relevant cases through the actual maintained
collector/runner/consumer and run the current repository gates. Preserve current
scope, source identity, safety/authorization and truthful unestablished claims.
