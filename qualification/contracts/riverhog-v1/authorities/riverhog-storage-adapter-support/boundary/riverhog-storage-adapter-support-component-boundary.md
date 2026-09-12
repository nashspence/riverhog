# riverhog-storage-adapter-support component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-storage-adapter-support:riverhog-storage-adapter-support-component-boundary:0d1479bc2e -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/12`

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
    "riverhog-storage-adapter-conformance": "riverhog_storage_adapter_support.conformance:main",
    "riverhog-storage-adapter-schemas": "riverhog_storage_adapter_support.schemas:main"
  },
  "dependencies": [
    "http-api-contracts",
    "riverhog-storage-adapter-protocol"
  ],
  "distribution": "riverhog-storage-adapter-support",
  "optional_dependencies": {},
  "path": "packages/riverhog-storage-adapter-support",
  "role": "reusable_library"
}
```
