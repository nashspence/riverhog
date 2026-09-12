# riverhog-storage-adapter-asgi-support component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-storage-adapter-asgi-support:riverhog-storage-adapter-asgi-support-com-c5256e8bd2:1a3ef4052a -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-asgi-support` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/10`

## Effective policies

- `boundary/frozen-authority/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`
- Proof: `make release-check`
- Proof: `make build`

## Contract

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts",
    "riverhog-storage-adapter-protocol",
    "riverhog-storage-adapter-support"
  ],
  "distribution": "riverhog-storage-adapter-asgi-support",
  "optional_dependencies": {},
  "path": "packages/riverhog-storage-adapter-asgi-support",
  "role": "reusable_library"
}
```
