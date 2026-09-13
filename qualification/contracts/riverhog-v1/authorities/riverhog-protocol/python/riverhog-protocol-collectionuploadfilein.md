# riverhog_protocol.CollectionUploadFileIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadfilein:f0086a248d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d65abb63f3"></a>
| Field | Shape |
|---|---|
| <a id="s-b84f70c91b"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-d05122f27b"></a>`distribution` | "riverhog-protocol" |
| <a id="s-267f254c58"></a>`module` | "riverhog_protocol" |
| <a id="s-cc2efd5537"></a>`name` | "CollectionUploadFileIn" |
| <a id="s-0c59057afa"></a>`unit` | "export" |

## Governing policies

- <a id="pa-2e4809c9bd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadFileIn`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a4987bc96353b04783756dccb9665c9b020c5c339e875fe3a6e8a18f3030c3d8 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "cf12da502a56d14e45c83fcd32b348851101ef6e564789667a5d2f3d2b2dc3e8",
    "signature": "\"(*, path: CanonicalRelPath, bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], raw_parts: riverhog_protocol.collection_upload_transport.CollectionUploadRawPartsIn | None = None, provenance: Optional[Annotated[riverhog_protocol.collection_upload_transport.CapturedFileProvenanceBinding | riverhog_protocol.collection_upload_transport.OmittedFileProvenanceBinding, FieldInfo(annotation=NoneType, required=True, discriminator='status')]] = None) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadFileIn",
  "unit": "export"
}
```
