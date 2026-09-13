# riverhog_protocol.ArchiveCopyStoreSelectionDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-archivecopystoreselectiondocument:2bbb4b7c7f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-30c690a6e0"></a>
| Field | Shape |
|---|---|
| <a id="s-d046463cac"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-e3c19e20cd"></a>`distribution` | "riverhog-protocol" |
| <a id="s-70ce011605"></a>`module` | "riverhog_protocol" |
| <a id="s-5943d1a0e0"></a>`name` | "ArchiveCopyStoreSelectionDocument" |
| <a id="s-f6a197f9d5"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.ArchiveCopyStoreSelectionDocument.validate_distinct_stores](riverhog-protocol-archivecopystoreselectiondocument-validate-distinct-stores.md)

## Governing policies

- <a id="pa-28b3131d1e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ArchiveCopyStoreSelectionDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 27ebfed4073d06517a067efb74a7eb057a161a73dc03accda04ad5613ec8cb7b -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "aa1475d23f33700b5b904b55f5cbd41fb7870cbfac5bfd49a98febfbba5dd246",
    "signature": "'(*, destination_store: ArchiveStoreName, source_store: ArchiveStoreName | None = None) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ArchiveCopyStoreSelectionDocument",
  "unit": "export"
}
```
