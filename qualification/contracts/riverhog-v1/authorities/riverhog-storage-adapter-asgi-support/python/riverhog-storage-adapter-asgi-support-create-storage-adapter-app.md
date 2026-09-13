# riverhog_storage_adapter_asgi_support.create_storage_adapter_app

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-asgi-support:riverhog-storage-adapter-asgi-support-cre-f8e940e4d2:9d2eabb8b4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-asgi-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e62100e1ee"></a>
| Field | Shape |
|---|---|
| <a id="s-f133c05e56"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-ca71fc76ad"></a>`distribution` | "riverhog-storage-adapter-asgi-support" |
| <a id="s-79809375ec"></a>`module` | "riverhog_storage_adapter_asgi_support" |
| <a id="s-6e37d8f9f6"></a>`name` | "create_storage_adapter_app" |
| <a id="s-8df0ae50ff"></a>`unit` | "export" |

## Governing policies

- <a id="pa-4f30976f88"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-asgi-support:riverhog_storage_adapter_asgi_support](../../../evidence/sources.md#src-faaefe65d4) — `packages/riverhog-storage-adapter-asgi-support/src/riverhog_storage_adapter_asgi_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_asgi_support.create_storage_adapter_app`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 25695cb23acf8335a165f9b1a5415deca1c64a3d7da5635fc8ea7830681b555d -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, service: 'str', token: 'str', adapter: 'StorageAdapterPort', readiness: 'Callable[[], None] | None' = None) -> 'FastAPI'\""
  },
  "distribution": "riverhog-storage-adapter-asgi-support",
  "module": "riverhog_storage_adapter_asgi_support",
  "name": "create_storage_adapter_app",
  "unit": "export"
}
```
