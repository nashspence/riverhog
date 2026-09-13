# riverhog_protocol.CollectionUploadWorkBatchDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadworkbatchdocument:2f3ce8bfee -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-da21e8fb67"></a>
| Field | Shape |
|---|---|
| <a id="s-dad825a1aa"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-043c8f59d5"></a>`distribution` | "riverhog-protocol" |
| <a id="s-149aa95b51"></a>`module` | "riverhog_protocol" |
| <a id="s-5d03b06b65"></a>`name` | "CollectionUploadWorkBatchDocument" |
| <a id="s-aa7fed4df7"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionUploadWorkBatchDocument.validate_completion](riverhog-protocol-collectionuploadworkbatchdocument-validate-completion.md)
- [riverhog_protocol.CollectionUploadWorkBatchDocument.canonical_collection_id](riverhog-protocol-collectionuploadworkbatchdocument-canonical-collection-id.md)

## Governing policies

- <a id="pa-cb75ce1ccd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadWorkBatchDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2b198bd9584a3e94be32513b3a953e0229b7144c03cc861a2ac79e4b31e1d3e6 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "7b7262b1082eac6f354ec27b4a93c860a618a7bf3d9f9cc1a6a04e22de23a9b8",
    "signature": "'(*, collection_id: CollectionId, planning_complete: bool, complete: bool, committed_payload_bytes: Annotated[int, Strict(strict=True), Ge(ge=0)], work: Annotated[list[riverhog_protocol.collection_upload_transport.CollectionUploadUnitAssignmentDocument], MaxLen(max_length=64)]) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadWorkBatchDocument",
  "unit": "export"
}
```
