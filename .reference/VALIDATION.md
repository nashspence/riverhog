# Validation and evidence limits

## Exact source relationship

Audited public base: `0e45d35a64d18dbde0f88b89d3c55d60b4977d28`.
Base tree: `c7250f2b49a3cc0bc8f6f8827b9d128a7e689537`.
The integration ref was read as that SHA. The issue's local unpublished
`d51db81deae3cd122ffc037b728429dac06ea4c3` was not available for inspection.
Only `.reference/` material is added; product paths and modes remain unchanged.
The clone ZIP SHA-256 was verified as
`50c10e1546387a8d975576753632e557d7e3366a4d51df7320f2f0d66228f6d4`.
Extracted executable bits were restored from the Git index, without content edits.

## Executed reference checks

Two separate wheels were compiled with GCC 14.2.0 and `-Wall -Wextra -Werror`:
a fault-witness build with `RETAINED_POSIX_TESTING=1`, and a normal build without
that macro. Each was installed into its own environment. Tests ran from outside
the checkout with `python -I`; product imports resolved to installed site-packages,
not an in-place extension. The normal smoke explicitly rejects a fault-hook API.
Exact wheel and native-extension hashes are recorded in [RESULT.json](RESULT.json).

The final installed fault-wheel run passed **26 tests**, zero failures/errors or
skips. Command elapsed time was **0.131357 seconds** (unittest body: 0.068 seconds).
These are actual Linux regular-file/directory opens, kind checks, duplication,
borrowed operations and closes. Test-owned descriptor-slot reuse includes an
unrelated file and a new open of the same inode. Native test seams distinguish
before-effect refusal, after-effect Python reply loss, and simulated unknown
native outcomes. Unknown-open tests execute in subprocesses to avoid leaking
probe resources into the test runner. Process exit is not counted as product
receiving. Python signal delivery is exercised after native state commitment.

The separate normal installed-wheel smoke passed in **0.042391 seconds**. It
confirms no checkout import or test-hook API, retained parent/file use, close-reply
loss/reuse safety, and a real three-byte neutral extent. It is an isolated
installed-package witness for this reference, not Riverhog's product dist-smoke.

The source-owned `growth.py` declarations were checked with the maintained
`riverhog-growth-contracts` validator at the audited source. **456 samples across
two declarations** validated in a command elapsed **0.118170 seconds**. Native
counter deltas, not method-success counts, establish at most one open/type-check
on acquisition and at most one close across the original lifetime; repeated close
receiving performs no open, dup or fstat. Unrelated populations 0/17/257 and file
extents 0/17/65,537/8,388,608 bytes were varied independently, with file and directory
owners and 17 repeated receives per resource. [GROWTH_RESULT.json](GROWTH_RESULT.json)
records the projection, maxima, sample counts and full validated stream digest.
[growth_probe.py](growth_probe.py) regenerates and prints the complete sample
stream. Its timing varies; the semantic sample stream is the hashed evidence.

The measured native Resource struct is 96 bytes on this build. This is not total
Python heap, allocator overhead, kernel-memory or aggregate retirement memory.
The declarations bound the native reference effects, not OS path resolution,
filesystem close latency, physical block release, or Core/Native end-to-end work.

## Failed attempts and environment

The initial 25-test fault run failed two cases because the test dispatcher consumed
a close fault during the preceding open. The dispatcher was corrected to consume
only the selected operation's seam. The two safety assertions were retained.
Later tests added the duplicate-parent witness and both file/directory signal
paths. The final 26-case run used the rebuilt installed wheel; no failure was
converted into a skip or a weaker custody condition.

Scratch build interpreter: CPython 3.13.5 (July 15 2026 build); installed test
interpreters: CPython 3.13.5 (June 25 2025 build), same cp313 ABI. Linux x86-64,
glibc 2.41, GCC 14.2.0, setuptools 82.0.1. The standalone reference packaging pin
matches this actual build environment, not repository-locked product policy.
Riverhog pins CPython 3.12.3 and its own toolchain. Darwin was not executed.

PyPI retrieval was unavailable. For the growth validator only, the exact
`rfc8785` 0.1.4 source was fetched through GitHub from `trailofbits/rfc8785.py` tag
`v0.1.4`; both Git blob identities were verified:
`__init__.py` = `5a1f9d919643fa3bcaa0999ea66d9c535568c42a`;
`_impl.py` = `3137d3326b98938affadb1be711ee411eb2ab86e`.
There was no alternate serializer or fail-on-use substitute. This source-only
scratch dependency plus the audited growth/canonical modules does not establish
installed product dependency or complete locked-runtime qualification.

`make lint` was attempted and stopped before lint execution because `mise` was
unavailable: make exit 2, prerequisite exit 127. No lint pass is claimed.

## Static and publication checks

Reference Python/TOML/JSON parsing, exact owning-source symbol checks, local
Markdown-link/newline/fence checks, source-only package inventory, and staged
whitespace/product-path checks are performed before publication. Actual static
counts and the read-back published parent/tree/ref/check status are recorded in
the owning issue. No generated wheel, native binary, object file, build directory
or dependency source is included in the branch.

## Not qualified by this reference

No maintained DiscoveryRetirementTurn reproduction was rerun; #987's two original
bug probes and their 9.062-second elapsed time remain prior integration evidence.
No Core/Native database/service integration, native macOS, repository-locked
CPython build, Windows implementation, full ordinary/terminal budget matrix,
whole product distribution, real kernel EINTR injection or complete retirement
qualification was executed. The normal Linux syscall boundary and reference
composition are executable evidence, not proof that all application owners have
already adopted them. [QUALIFICATION.md](QUALIFICATION.md) retains those obligations.

Unknown native outcomes intentionally have no automatic positive in-process
receipt. The original resource charge and parent custody stay retained. A model
or test-driver decision cannot manufacture that evidence. Publication does not
activate discovery maintenance or authorize issue completion, merge or release.
