# riverhog_storage_adapter_asgi_support

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-asgi-support:riverhog-storage-adapter-asgi-support:a90756b038 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-asgi-support` |
| Interface | `python` |
| Family | `modules` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/python/10`

## Effective policies

- `compatibility/python-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `python:riverhog-storage-adapter-asgi-support` — `packages/riverhog-storage-adapter-asgi-support/src/riverhog_storage_adapter_asgi_support/__init__.py::<module>`
- Proof: `make dist-smoke`
- Proof: `make build`

## Contract

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
