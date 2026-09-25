# riverhog-read-collection-progression/v1: candidate tests

[Atlas](../../../index.md) · [Reference navigation](index.md)

Source bindings and candidate tests are audit leads. This checked-in atlas contains no executed qualification result or CI attestation. A passing run applies only to its executed checks and exact source SHA; main CI does not imply release or provider qualification.

A reviewed scope states what a test exercises. A candidate binding without a reviewed scope makes no behavioral proof claim.

Candidate commands: [make unit](../../sources/commands.md#q-ce47068f50), [make postgres-concurrency](../../sources/commands.md#q-8d9f4da038), [make database-qualification](../../sources/commands.md#q-27f281b51e).

- [tests/unit/test\_public\_interface\_parity.py::test\_public\_read\_collection\_selectors\_are\_bounded\_and\_frozen](../../../../../../tests/unit/test_public_interface_parity.py#L743) — Structural OpenAPI checks for bounded read selectors; no traversal is executed.
- [tests/unit/test\_collection\_reads.py::test\_collection\_list\_query\_count\_is\_independent\_of\_page\_rows](../../../../../../tests/unit/test_collection_reads.py#L90) — Collection-list service query count and archive-object loading on one page; does not test traversal or restart.
- [tests/unit/test\_collection\_reads.py::test\_collection\_encryption\_filters\_preserve\_catalog\_authorization](../../../../../../tests/unit/test_collection_reads.py#L132) — Collection-list service encryption filters and catalog authorization on one page.
- [packages/http-api-contracts/tests/test\_browse\_tokens.py::test\_browse\_token\_round\_trips\_opaque\_binary\_position\_across\_restart](../../../../../../packages/http-api-contracts/tests/test_browse_tokens.py#L23) — Shared token codec reconstructs a position after codec recreation with the same signing configuration; does not prove route wiring or database traversal.
- [packages/http-api-contracts/tests/test\_browse\_tokens.py::test\_browse\_token\_fails\_closed\_outside\_its\_request\_binding](../../../../../../packages/http-api-contracts/tests/test_browse_tokens.py#L52) — Shared token codec rejects changed operation, principal, and selectors; does not prove each route supplies those bindings correctly.
- [tests/unit/test\_operation\_lifecycle\_api.py::test\_riverhog\_official\_client\_positive\_disposable\_lifecycle](../../../../../../tests/unit/test_operation_lifecycle_api.py#L232) — Real API and official client lifecycle includes a one-page collection list; its restart assertion concerns events, not collection-list continuation.
- [tests/unit/test\_cli\_json\_output.py::test\_collection\_list\_json\_emits\_the\_api\_response\_without\_a\_second\_model](../../../../../../tests/unit/test_cli_json_output.py#L13) — Collection-list CLI accepts a current-schema fixture in both output modes, preserves the entire JSON response, and displays its description and encryption in human output; uses a fake client and does not establish general output parity.
- [tests/unit/test\_catalog\_sync.py::test\_catalog\_sync\_crosses\_many\_pages\_and\_repairs\_fixed\_frontier\_changes](../../../../../../tests/unit/test_catalog_sync.py#L238) — Catalog-sync fixed-frontier traversal and repair; ordinary mutable collection browsing has different semantics and gains no snapshot-completeness claim.
- [tests/integration/test\_lifecycle\_event\_concurrency.py::test\_event\_reads\_and\_concurrent\_context\_reapers\_do\_only\_bounded\_work](../../../../../../tests/integration/test_lifecycle_event_concurrency.py#L74) — Lifecycle-event reads and concurrent context reaping; not collection browsing.
