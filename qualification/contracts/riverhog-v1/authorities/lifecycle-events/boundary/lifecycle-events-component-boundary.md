# lifecycle-events component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:lifecycle-events:lifecycle-events-component-boundary:dc11b5cdd5 -->

| Audit field | Value |
|---|---|
| Authority | `lifecycle-events` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/2`

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
  "distribution": "lifecycle-events",
  "optional_dependencies": {},
  "path": "packages/lifecycle-events",
  "role": "reusable_library"
}
```
