# Finite qualification and consumption

## Reference commands

Run in a separate environment, not inside a user's live namespace. The fault
build uses `RETAINED_POSIX_TESTING=1`; the normal wheel must omit its `_test_fault`
method. `python setup.py bdist_wheel` is a standalone reference fixture, not a new
repository developer command or packaging-policy exception. Install each wheel
into a distinct environment and run the scripts with `python -I` from outside
the checkout. Do not import an in-place extension while claiming wheel evidence.

- `retained-posix/tests/test_receiving.py`: actual files/directories plus native
  fault seams, including test-owned descriptor reuse and child-process isolation.
- `retained-posix/tests/installed_smoke.py`: normal installed wheel, no fault API,
  no checkout path, directory/file acquisition, lost caller close reply, reuse,
  and the neutral retained three-byte extent.
- `growth_probe.py`: native counters through the maintained growth declaration
  validator. Use the repository's installed growth/canonical dependencies; no
  alternate serializer is permitted. It emits the validated counts and projection.

## Decisive maintained-owner vectors

| Boundary | Required evidence |
| --- | --- |
| Constructor/allocation failure | No acquisition in constructor; caller's already-held admission cannot vanish because constructing a new return object failed. |
| Before native acquisition | Known pre-dispatch refusal makes no open/dup; original retained owner is still the retry target. |
| After open/dup, before Python reply | Same original object retains its acquired resource; no second acquisition. |
| Post-acquisition validation failure | Acquired but rejected file/directory remains owned until explicit close receiving. |
| Before native close | Refusal keeps the same live resource and every predecessor. No record is popped. |
| Close returned zero, reply lost | Retry dispatches zero further closes. Exact recycled slot remains untouched for both an unrelated file and the same inode reopened. |
| Native close error / unknown acquisition | Remain charged and deny redispatch, new borrowing and parent receiving. Include open and actually-closed error outcomes as negative controls. |
| Python signal after native effect | State is recorded before handler exception; retry reads that state, never probes/reopens an integer. |
| Active borrower / actual worker join | Close refuses while borrow is held; actual join alone is insufficient until that exact borrower and resource receive. |
| Duplicated lease | Source close does not receive the duplicate or its parent dependency; no replacement pathname is used. |
| Group capacity / receiving quantum | Admission precedes acquisition; receive at most the admitted record budget, stop at first unresolved child, preserve predecessor charges. |
| Metadata three-byte extent | Actual data close refusal after truncation leaves the retained data owner reachable in the original Core turn. Close-only retry does not truncate again. |
| Extent fsync failure | Descriptor receiving does not turn the failed mutation/durability operation into success. |
| Parent close / namespace cleanup | Lost parent close reply cannot close a recycled file. Do not delete catalog/claim receiving on a false `_closed` or missing filename. |
| Child/corpus/Core routing | Native owner receiving, neutral extent receiving, Core DB receiving and parent lease receiving remain separate and ordered. |
| Ordinary/terminal resource exhaustion | Unknown owners retain the original charge; independent admitted work can progress without lending reserved terminal capacity to new work. |

The supplied native fault seams distinguish before-effect refusal, lost Python
reply, and simulated unknown native outcome. They are not evidence that a real
kernel produced EINTR/EIO in that run. The actual EBADF breach control is also
not a supported way to manufacture receiving. Never run reuse probes on handles
which the test does not own. Test-owned unknown-open cases run in subprocesses
so their resources are cleaned at process exit without inventing product receipts.

## Platform and distribution matrix

Execute on repository-locked CPython, native Linux and native macOS, both source
and independent installed package. Verify actual built extension identity,
O_CLOEXEC, relative no-follow opens, supported filesystem behavior, and real
private file/directory mutation under existing custody. Windows should explicitly
report unsupported for this POSIX package; that does not reduce a separate
Windows product contract or count as a qualified Windows implementation.

Preserve the lock/build policy and add the new package to maintained dependency,
workspace boundary, distribution, public interface and growth gates. Native code
must not become an untested sidecar. Run focused tests, lint, required exact-source
and tested-merge CI according to AGENTS.md. Publication of this reference creates
no new gate exemption.

## Growth proof placement

Move the reference native syscall counters/declarations into the maintained
shared owner's measurement/declaration machinery. Include successful, refused,
interrupted and idempotent paths; do not count only returned successful methods.
Qualify file extents and unrelated namespace populations independently, plus
retained resource counts and close-step sizes. Count attempts and kernel entries,
not bytes inferred from a filename. Keep kernel/FS latency and allocated blocks
separate from logical extents and user-space syscall bounds.

Retain the source name-sort/metadata growth obligations of workspace-reclamation;
this reference qualifies only the new resource boundary with its declarations.
The reference's constant native struct size does not prove aggregate process
memory, SQLite cache behavior, or a universal bound on filesystem close latency.

## Consumption record

Record adoption/reproduction/partial use/supersession/rejection briefly in #987
with exact integrated source, original red evidence, final tests/growth/timing,
and remaining platform or product obligations. Existing neutral extraction is
already authorized; routine integration/packaging remains with its owner. Do not
reopen #964's architecture or treat this branch as an alternate implementation.
