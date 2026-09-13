# riverhog_protocol.CollectionUploadArtifactCustodyReceiptDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadartifac-1c46d59ee4:1fc5e1c7cf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-088a252182"></a>
| Field | Shape |
|---|---|
| <a id="s-c77b9e9f16"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-fe7d236555"></a>`distribution` | "riverhog-protocol" |
| <a id="s-f190b93197"></a>`module` | "riverhog_protocol" |
| <a id="s-e1b1f2d405"></a>`name` | "CollectionUploadArtifactCustodyReceiptDocument" |
| <a id="s-be3dd24fac"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionUploadArtifactCustodyReceiptDocument.seal](riverhog-protocol-collectionuploadartifactcustodyreceiptdocument-seal.md)
- [riverhog_protocol.CollectionUploadArtifactCustodyReceiptDocument.validate_receipt](riverhog-protocol-collectionuploadartifactcustodyreceiptdocument-validate-receipt.md)
- [riverhog_protocol.CollectionUploadArtifactCustodyReceiptDocument.canonical_path](riverhog-protocol-collectionuploadartifactcustodyreceiptdocument-canonical-path.md)
- [riverhog_protocol.CollectionUploadArtifactCustodyReceiptDocument.canonical_collection_id](riverhog-protocol-collectionuploadartifactcustodyreceiptdocument-canonical-collection-id.md)

## Governing policies

- <a id="pa-503617e5f3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadArtifactCustodyReceiptDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 604a73fde5b26678b60dfe2ef2af82ee188ca2e039159c91782b391118a703f4 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "ca6c2ec6a435c6e5819f60a47a7a093f2368aaba5af7fdecda499cf6235fbec3",
    "signature": "\"(*, format: Literal['riverhog-artifact-custody-receipt/v1'] = 'riverhog-artifact-custody-receipt/v1', collection_id: CollectionId, path: str, bytes: Annotated[int, Strict(strict=True), Ge(ge=0)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], archive_object_count: Annotated[int, Strict(strict=True), Ge(ge=1)], archive_object_set_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], receipt_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadArtifactCustodyReceiptDocument",
  "unit": "export"
}
```
