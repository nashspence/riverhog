# Evidence gaps across pages or chunks

[Atlas](../../index.md) · [Reference navigation](../sources.md)

The named contract groups have recorded evidence gaps in the following guarantees. Each group's page identifies its exact open guarantees and candidate tests:

- Each step stays within its declared limits.
- Continuing the work makes progress toward its declared completion.
- The operation works across multiple pages or chunks.
- Required data or work is not silently left out.
- Work can resume after a restart as its contract requires.

These guarantees let large tasks proceed in smaller steps: a limit on one page or chunk must not become a hidden limit on the whole task. Returning a first page correctly does not establish that continuation or recovery works. Capacity may explicitly reject, defer, or throttle work; it must not silently omit work.

Existing tests may establish individual cases. The gaps retain their recorded group-wide scope and do not establish a bug in every linked contract. Completion follows each contract's rules; mutable browsing carries no implied snapshot guarantee.

Each group has separate routes to its affected contracts and candidate tests. Test-symbol existence and owner/reason matching validate routing only.

| Evidence group | Bound extent decisions | Candidate tests |
|---|---:|---:|
| [riverhog-upload-work-progression/v1](riverhog-upload-work-progression-v1/index.md) | 1 | 3 |
| [riverhog-archive-volume-part-progression/v1](riverhog-archive-volume-part-progression-v1/index.md) | 2 | 3 |
| [riverhog-storage-write-segment-progression/v1](riverhog-storage-write-segment-progression-v1/index.md) | 1 | 3 |
| [riverhog-work-set-append/v1](riverhog-work-set-append-v1/index.md) | 2 | 2 |
| [riverhog-work-disposition-append/v1](riverhog-work-disposition-append-v1/index.md) | 2 | 2 |
| [riverhog-provenance-volume-progression/v1](riverhog-provenance-volume-progression-v1/index.md) | 1 | 3 |
| [riverhog-raw-digest-progression/v1](riverhog-raw-digest-progression-v1/index.md) | 1 | 2 |
| [riverhog-retrieval-work-progression/v1](riverhog-retrieval-work-progression-v1/index.md) | 1 | 2 |
| [riverhog-read-collection-progression/v1](riverhog-read-collection-progression-v1/index.md) | 54 | 9 |
| [stove0-read-collection-progression/v1](stove0-read-collection-progression-v1/index.md) | 12 | 3 |
| [a-riverhog-ftp-spool-status-progression/v1](a-riverhog-ftp-spool-status-progression-v1/index.md) | 1 | 17 |
| [riverhog-upload-registration-progression/v1](riverhog-upload-registration-progression-v1/index.md) | 1 | 2 |
| [riverhog-upload-tag-staging-progression/v1](riverhog-upload-tag-staging-progression-v1/index.md) | 2 | 8 |
| [riverhog-upload-unit-source-progression/v1](riverhog-upload-unit-source-progression-v1/index.md) | 1 | 3 |
