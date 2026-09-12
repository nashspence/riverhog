# riverhog-server component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-server:riverhog-server-component-boundary:ed9704d0e8 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-server` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/70`

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
    "riverhog-api": "riverhog_api.app:main"
  },
  "dependencies": [
    "http-api-contracts",
    "lifecycle-events",
    "riverhog-age",
    "riverhog-application-access",
    "riverhog-archive-contracts",
    "riverhog-protocol",
    "riverhog-provenance",
    "riverhog-provenance-contracts",
    "riverhog-storage-adapter-protocol",
    "riverhog-storage-adapter-support",
    "state-schema",
    "time-formats"
  ],
  "distribution": "riverhog-server",
  "optional_dependencies": {},
  "path": "riverhog",
  "role": "deployed_implementation"
}
```
