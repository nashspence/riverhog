# riverhog-storage-adapter-protocol component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-component-boundary:161627301d -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-protocol` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/11`

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
    "time-formats"
  ],
  "distribution": "riverhog-storage-adapter-protocol",
  "optional_dependencies": {},
  "path": "packages/riverhog-storage-adapter-protocol",
  "role": "reusable_library"
}
```
