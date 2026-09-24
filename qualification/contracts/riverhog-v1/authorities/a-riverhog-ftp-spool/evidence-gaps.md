# a-riverhog-ftp-spool: evidence gaps

[Atlas](../../index.md) · [Reference navigation](index.md)

This view covers **4 affected contract elements** within a-riverhog-ftp-spool. Only their recorded evidence groups are included.

The named contract groups have recorded evidence gaps in the following guarantees. Each group's page identifies its exact open guarantees and candidate tests:

- Each step stays within its declared limits.
- Continuing the work makes progress toward its declared completion.
- The operation works across multiple pages or chunks.
- Required data or work is not silently left out.
- Work can resume after a restart as its contract requires.

These guarantees let large tasks proceed in smaller steps: a limit on one page or chunk must not become a hidden limit on the whole task. Returning a first page correctly does not establish that continuation or recovery works. Capacity may explicitly reject, defer, or throttle work; it must not silently omit work.

Existing tests may establish individual cases. The gaps retain their recorded group-wide scope and do not establish a bug in every linked contract. Completion follows each contract's rules; mutable browsing carries no implied snapshot guarantee.

The table identifies the exact evidence groups for each element. Its element link opens the local explanation and governing rules; each evidence group gives its specific open guarantees and candidate tests.

| Contract element | Evidence group |
|---|---|
| [a-riverhog-ftp-spool: GET /v1/sources/{source_id}/events](http-operations/get-v1-sources-source-id-events.md#evidence-gaps) | [a-riverhog-ftp-spool-read-collection-progression/v1](../../evidence/qualifications/a-riverhog-ftp-spool-read-collection-progression-v1/index.md) |
| [a-riverhog-ftp-spool: GET /v1/status](http-operations/get-v1-status.md#evidence-gaps) | [a-riverhog-ftp-spool-read-collection-progression/v1](../../evidence/qualifications/a-riverhog-ftp-spool-read-collection-progression-v1/index.md) |
| [a-riverhog-ftp-spool: schemas: FtpEventPage](http-schemas/schemas-ftpeventpage.md#evidence-gaps) | [a-riverhog-ftp-spool-read-collection-progression/v1](../../evidence/qualifications/a-riverhog-ftp-spool-read-collection-progression-v1/index.md) |
| [a-riverhog-ftp-spool: schemas: FtpSpoolStatus](http-schemas/schemas-ftpspoolstatus.md#evidence-gaps) | [a-riverhog-ftp-spool-read-collection-progression/v1](../../evidence/qualifications/a-riverhog-ftp-spool-read-collection-progression-v1/index.md) |
