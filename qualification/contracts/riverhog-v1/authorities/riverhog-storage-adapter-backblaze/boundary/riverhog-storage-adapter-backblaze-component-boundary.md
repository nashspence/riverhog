# riverhog-storage-adapter-backblaze component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-storage-adapter-backblaze:riverhog-storage-adapter-backblaze-compon-5a06186e8e:7defe9e1e1 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-backblaze` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/37`

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
  "console_scripts": {
    "riverhog-storage-adapter-backblaze": "riverhog_storage_adapter_backblaze.app:main"
  },
  "dependencies": [
    "riverhog-storage-adapter-asgi-support",
    "riverhog-storage-adapter-s3-support"
  ],
  "distribution": "riverhog-storage-adapter-backblaze",
  "optional_dependencies": {},
  "path": "reference/riverhog/storage/backblaze",
  "role": "reference_component"
}
```
