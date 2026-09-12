# riverhog-storage-adapter-s3-support component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-storage-adapter-s3-support:riverhog-storage-adapter-s3-support-compo-843346b56b:cbc3e1878a -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-s3-support` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/39`

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
    "riverhog-storage-adapter-protocol",
    "time-formats"
  ],
  "distribution": "riverhog-storage-adapter-s3-support",
  "optional_dependencies": {},
  "path": "reference/riverhog/storage/s3-support",
  "role": "reference_component"
}
```
