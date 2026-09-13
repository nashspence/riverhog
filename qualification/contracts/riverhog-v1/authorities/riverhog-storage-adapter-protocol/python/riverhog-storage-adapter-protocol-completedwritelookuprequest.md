# riverhog_storage_adapter_protocol.CompletedWriteLookupRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-complet-92915a63c7:6d8815fa61 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d3d0e65ce0"></a>
| Field | Shape |
|---|---|
| <a id="s-f4da3af53b"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-bd89ea24e3"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-f837b0436c"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-aa8062fbd2"></a>`name` | "CompletedWriteLookupRequest" |
| <a id="s-d71c948c8c"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.CompletedWriteLookupRequest.canonical_path](riverhog-storage-adapter-protocol-completedwritelookuprequest-canonical-path.md)
- [riverhog_storage_adapter_protocol.CompletedWriteLookupRequest.canonical_metadata](riverhog-storage-adapter-protocol-completedwritelookuprequest-canonical-metadata.md)

## Governing policies

- <a id="pa-9da211f6e9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.CompletedWriteLookupRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2a1b5bce30803450477a753a1cd9379868d7e4474b3177d770b926c5f0e9d9b9 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "3f6289ea7cdb8562a130c3d3df28e4217ae0490a38d84f2f2158dafb98e18689",
    "signature": "\"(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], expected_bytes: Annotated[int, Ge(ge=1)], expected_content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], required_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], expected_placement: Literal['archive', 'immediate']) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "CompletedWriteLookupRequest",
  "unit": "export"
}
```
