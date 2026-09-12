# piggity component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:piggity:piggity-component-boundary:43cd09ec8c -->

| Audit field | Value |
|---|---|
| Authority | `piggity` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/26`

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
    "piggity": "piggity.main:main"
  },
  "dependencies": [
    "http-api-contracts",
    "riverhog-application-access",
    "riverhog-client",
    "riverhog-protocol",
    "riverhog-provenance",
    "state-schema",
    "time-formats"
  ],
  "distribution": "piggity",
  "optional_dependencies": {},
  "path": "reference/riverhog/applications/piggity",
  "role": "reference_application"
}
```
