# Producer validation and limitations

## Audited source and transfer

Repository: `nashspence/riverhog`
Source commit: `6e8f93b33ea09439ad7c1e94f1907e2aea3a0f43`
Source tree: `33d3d94721495fe1905dd844f03b254dcdc4e407`
Full-clone archive SHA-256: `3587f7aa22eebb0fba60ac1fdde918b7f4932ce07b93304943a2b2f5ebd47f72`
Manifest SHA-256: `ee6967b8902b4793b8c438898070e2fa4788ef8d3df952b6ab7c6e368bc75ba3`

The archive and manifest checksums were verified. The clone contains full history; HEAD/tree were checked with Git and `git fsck --full` completed without reported errors. ZIP extraction did not initially restore executable mode bits; the manifest's executable modes were restored before source cleanliness checks. Git may refresh its local `.git/index`; that local metadata change is not a tracked-source difference. Product paths were not edited.

## Checks actually performed before publication

- Read source, current owning/adjacent issues, accepted policy, protocol and release/workspace declarations.
- Ran the standard-library reference inventory against the audited source. It reconciled 38 scoped distributions, 20 console source targets and 10 image declarations. Its observations show only the NVENC target entrypoint currently advertises `--execution-lease-file`.
- Ran `RIVERHOG_REFERENCE_ROOT=<audited-root> python .reference/test_audit_inventory.py -v` using Python 3.13.5: **14 tests passed**, no skipped test with that root configured. These test the reference reader/coverage guard, not the product runtime.
- Parsed the proposed modified HTTP test source and ran `git apply --check` for `role_http_parity.patch` against the exact audited base. Both checks passed. The patch adds the three omitted observer factories and an intended failing regression for magic's undeclared readiness 503. The repository tests in that patch were **not executed**.

Publication tree/content/ancestry verification, any post-publication rerun and the actual durable reference SHA are recorded in the owning issue publication comment. No pre-publication local commit identity is represented as the published GitHub commit.

## Not performed

No imported or executed repository product modules. No repository-wide lint/unit/distribution/build gate. No ASGI product test execution, Docker/container/host bind-mount run, Linux deployed restart qualification, PostgreSQL qualification, real FFmpeg/libmagic/ExifTool/Rclone operation, GPU encode, provider access, release qualification or production deployment.

No claim that current main implements the proposed conventions, that every code path was audited, or that a static absence proves every runtime consequence. Findings distinguish confirmed source shape from failure scenarios still requiring reproduction. Reference validation is not issue completion, product proof or integration permission.

## Reproduction

From the reference checkout, `python .reference/audit_inventory.py --root . --output /tmp/stove0-source-observations.json` produces source observations without importing the product. Run the reader tests with `RIVERHOG_REFERENCE_ROOT=$PWD python .reference/test_audit_inventory.py -v`. The snapshot-count test deliberately describes the audited source and is not a future production gate. Integration should use the maintained discovery/collection machinery for live membership checks.

After reconciling the patch, use the repository's existing supported test/gate invocation. Do not run a fake encoder and label it real native/hardware evidence; do not treat a test that accurately reports missing coverage as passing product conformance.
