# riverhog_storage_adapter_asgi_support

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-asgi-support:riverhog-storage-adapter-asgi-support:a90756b038 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-asgi-support` |
| Interface | `python` |
| Family | `modules` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `distribution` | "riverhog-storage-adapter-asgi-support" |
| `exports` | additional keys=`create_storage_adapter_app` |
| `module` | "riverhog_storage_adapter_asgi_support" |

## Governing policies

- `compatibility/python-api/v1`

## Evidence

### Qualification

- `make dist-smoke`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `python:riverhog-storage-adapter-asgi-support` — `packages/riverhog-storage-adapter-asgi-support/src/riverhog_storage_adapter_asgi_support/__init__.py::<module>`

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
