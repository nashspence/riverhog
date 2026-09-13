# riverhog_storage_adapter_asgi_support

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-asgi-support:riverhog-storage-adapter-asgi-support:3ca080c237 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-asgi-support](../index.md) |
| Interface | [python](index.md) |
| Family | [modules](index.md#f-e58ca8b8b8) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-920c0b4cb8"></a>
| Field | Shape |
|---|---|
| <a id="s-011df6a52b"></a>`candidate_id` | "python:riverhog-storage-adapter-asgi-support:riverhog_storage_adapter_asgi_support" |
| <a id="s-474a4ca1b2"></a>`distribution` | "riverhog-storage-adapter-asgi-support" |
| <a id="s-50a7fbdf61"></a>`exports` | additional keys=`create_storage_adapter_app` |
| <a id="s-98088c3203"></a>`module` | "riverhog_storage_adapter_asgi_support" |

## Governing policies

- <a id="pa-13733e00bd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-asgi-support:riverhog_storage_adapter_asgi_support](../../../evidence/sources.md#src-faaefe65d4) — `packages/riverhog-storage-adapter-asgi-support/src/riverhog_storage_adapter_asgi_support/__init__.py`

### Machine authority

- `/external_contract/python/25`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 101162d7bbd2e009cec18dd9d9b91f4a496082e824bb91e1532a82392daf3d4d -->

```json
{
  "candidate_id": "python:riverhog-storage-adapter-asgi-support:riverhog_storage_adapter_asgi_support",
  "distribution": "riverhog-storage-adapter-asgi-support",
  "exports": {
    "create_storage_adapter_app": {
      "kind": "function",
      "signature": "\"(*, service: 'str', token: 'str', adapter: 'StorageAdapterPort', readiness: 'Callable[[], None] | None' = None) -> 'FastAPI'\""
    }
  },
  "module": "riverhog_storage_adapter_asgi_support"
}
```
