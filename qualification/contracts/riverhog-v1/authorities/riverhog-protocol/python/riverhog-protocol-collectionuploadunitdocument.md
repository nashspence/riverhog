# riverhog_protocol.CollectionUploadUnitDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadunitdocument:6830ef1875 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-203eb98a36"></a>
| Field | Shape |
|---|---|
| <a id="s-be59ee01cc"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-8567377bf2"></a>`distribution` | "riverhog-protocol" |
| <a id="s-6dc76405e5"></a>`module` | "riverhog_protocol" |
| <a id="s-92121b4355"></a>`name` | "CollectionUploadUnitDocument" |
| <a id="s-05ba1be1a8"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionUploadUnitDocument.validate_sources](riverhog-protocol-collectionuploadunitdocument-validate-sources.md)

## Governing policies

- <a id="pa-1ce1353a3e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadUnitDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 334c21143a3676d8d2e0e72879717d02d1f624b7399cecd6b91b81df0f3371cc -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "140a215e31b017d029bdc313c3d6ed9fd5c86eae5501e54e66a121ea513e71df",
    "signature": "'(*, unit: Annotated[int, Ge(ge=0)], payload_bytes: Annotated[int, Strict(strict=True), Ge(ge=0)], plaintext_bytes: Annotated[int, Strict(strict=True), Ge(ge=0)], sources: Annotated[list[riverhog_protocol.collection_upload_transport.CollectionUploadUnitSourceDocument], MaxLen(max_length=1000)]) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadUnitDocument",
  "unit": "export"
}
```
