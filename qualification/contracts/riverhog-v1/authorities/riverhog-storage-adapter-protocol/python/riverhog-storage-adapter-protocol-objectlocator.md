# riverhog_storage_adapter_protocol.ObjectLocator

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-objectlocator:e847ba3146 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ac76af497d"></a>
| Field | Shape |
|---|---|
| <a id="s-0ed54af079"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-31923cc8a4"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-0017528ac7"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-dde4b58abe"></a>`name` | "ObjectLocator" |
| <a id="s-9fdc9f1df3"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.ObjectLocator.canonical_path](riverhog-storage-adapter-protocol-objectlocator-canonical-path.md)

## Governing policies

- <a id="pa-0c04b8db4c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ObjectLocator`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e06fa7eab3c3957b0c6dd2c3288dc47eadf6071ce81866dc668e8ae720c99dc9 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "5728d2076704f8b4113eabaab445c0f91b667082021eab79d06d5f97438ce1cb",
    "signature": "'(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], revision: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=2000)] = None) -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ObjectLocator",
  "unit": "export"
}
```
