# riverhog_storage_adapter_asgi_support

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-asgi-support:riverhog-storage-adapter-asgi-support:a90756b038 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-asgi-support](../index.md) |
| Interface | [python](index.md) |
| Family | [modules](index.md#f-e58ca8b8b8) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-ffe8aa2870"></a>
| Field | Shape |
|---|---|
| <a id="s-4fc3a70bdd"></a>`distribution` | "riverhog-storage-adapter-asgi-support" |
| <a id="s-f8aa066280"></a>`exports` | additional keys=`create_storage_adapter_app` |
| <a id="s-a7a31a9565"></a>`module` | "riverhog_storage_adapter_asgi_support" |

## Governing policies

- <a id="pa-aad1bc1d85"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-asgi-support](../../../evidence/sources.md#src-de39c18ba5) — `packages/riverhog-storage-adapter-asgi-support/src/riverhog_storage_adapter_asgi_support/__init__.py::<module>`

### Machine authority

- `/external_contract/python/10`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ec21d3ec74b2c48d90569fc773a9fc36f4e2201001303f47a259a237e4e7bb3e -->

```json
{
  "distribution": "riverhog-storage-adapter-asgi-support",
  "exports": {
    "create_storage_adapter_app": {
      "kind": "function",
      "signature": "(*, service: 'str', token: 'str', adapter: 'StorageAdapterPort', readiness: 'Callable[[], None] | None' = None) -> 'FastAPI'"
    }
  },
  "module": "riverhog_storage_adapter_asgi_support"
}
```
