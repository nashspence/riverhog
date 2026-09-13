# riverhog_storage_adapter_protocol.ReadReady

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-readready:2778027f28 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f6acfd0ad5"></a>
| Field | Shape |
|---|---|
| <a id="s-a3eae11bb4"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-353ecf5157"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-507f34c2cc"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-a9c9267975"></a>`name` | "ReadReady" |
| <a id="s-35078adbd3"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.ReadReady.canonical_available_until](riverhog-storage-adapter-protocol-readready-canonical-available-until.md)

## Governing policies

- <a id="pa-670b65e44f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ReadReady`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f4f74e926f42e6c51f2390e7e47162a15df67f712e17037555492c647f903fea -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "e82ea0df8857c679ccf90afa1702480b6aca83c3226500f6805d2ae25099ba53",
    "signature": "\"(*, state: Literal['ready'] = 'ready', available_until: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=100)] = None) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ReadReady",
  "unit": "export"
}
```
