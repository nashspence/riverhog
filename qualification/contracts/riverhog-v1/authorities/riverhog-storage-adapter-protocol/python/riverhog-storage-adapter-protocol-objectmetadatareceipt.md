# riverhog_storage_adapter_protocol.ObjectMetadataReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-objectm-d3d9685026:f026627130 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d6bd7e4662"></a>
| Field | Shape |
|---|---|
| <a id="s-737b818316"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-4b684e98e6"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-310b6feb81"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-9e04636697"></a>`name` | "ObjectMetadataReceipt" |
| <a id="s-d077994d56"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.ObjectMetadataReceipt.canonical_completed_at](riverhog-storage-adapter-protocol-objectmetadatareceipt-canonical-completed-at.md)
- [riverhog_storage_adapter_protocol.ObjectMetadataReceipt.canonical_metadata](riverhog-storage-adapter-protocol-objectmetadatareceipt-canonical-metadata.md)
- [riverhog_storage_adapter_protocol.ObjectMetadataReceipt.canonical_path](riverhog-storage-adapter-protocol-objectmetadatareceipt-canonical-path.md)

## Governing policies

- <a id="pa-24799e3c3e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ObjectMetadataReceipt`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ef9612e281a90be2250ce6a7159ade0098a3dfee1ff8dd3b36c64f2d151cfa51 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "10fe081f0530594bffeb9d17545b2652dd8db57725461bb29fe06922e61a7c67",
    "signature": "\"(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], revision: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=2000)] = None, entity_token: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=4000)] = None, content_type: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=255)] = None, stored_bytes: Annotated[int, Ge(ge=0)], stored_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, observed_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], verified_placement: Literal['archive', 'immediate'], completed_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=100)]) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ObjectMetadataReceipt",
  "unit": "export"
}
```
