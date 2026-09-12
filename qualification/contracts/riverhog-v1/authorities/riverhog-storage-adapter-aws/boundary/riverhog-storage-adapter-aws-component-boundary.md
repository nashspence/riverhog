# riverhog-storage-adapter-aws component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-storage-adapter-aws:riverhog-storage-adapter-aws-component-boundary:41c3df727d -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-aws` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/36`

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
    "riverhog-storage-adapter-aws": "riverhog_storage_adapter_aws.app:main"
  },
  "dependencies": [
    "riverhog-storage-adapter-asgi-support",
    "riverhog-storage-adapter-protocol",
    "riverhog-storage-adapter-s3-support",
    "time-formats"
  ],
  "distribution": "riverhog-storage-adapter-aws",
  "optional_dependencies": {},
  "path": "reference/riverhog/storage/aws",
  "role": "reference_component"
}
```
