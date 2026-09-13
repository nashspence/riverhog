# riverhog_protocol.CollectionUploadFileBatchDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadfilebatchdocument:173ab38ab6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4002f1fbf9"></a>
| Field | Shape |
|---|---|
| <a id="s-bfb26a0840"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-137799159a"></a>`distribution` | "riverhog-protocol" |
| <a id="s-480ca2826c"></a>`module` | "riverhog_protocol" |
| <a id="s-b71e351082"></a>`name` | "CollectionUploadFileBatchDocument" |
| <a id="s-ec11c5c66a"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionUploadFileBatchDocument.validate_unique_file_paths](riverhog-protocol-collectionuploadfilebatchdocument-validate-unique-file-paths.md)

## Governing policies

- <a id="pa-efce5a676f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadFileBatchDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 21bfe751068cfb0caeb83e64679b43bac1bafab4fb972e716752027892f6f216 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "aad7ffcd6f9b4328b1c183b23364fdfffbc5600bcde529a3265fb50f2356e501",
    "signature": "'(*, files: Annotated[list[riverhog_protocol.collection_upload_transport.CollectionUploadFileIn], MinLen(min_length=1), MaxLen(max_length=100)]) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadFileBatchDocument",
  "unit": "export"
}
```
