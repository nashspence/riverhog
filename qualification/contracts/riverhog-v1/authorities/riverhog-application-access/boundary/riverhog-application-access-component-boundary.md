# riverhog-application-access component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-application-access:riverhog-application-access-component-boundary:529bb88f14 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-application-access` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/4`

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
    "riverhog-protocol"
  ],
  "distribution": "riverhog-application-access",
  "optional_dependencies": {},
  "path": "packages/riverhog-application-access",
  "role": "reusable_library"
}
```
