# stove0-target-support component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-target-support:stove0-target-support-component-boundary:a106e9e41a -->

| Audit field | Value |
|---|---|
| Authority | `stove0-target-support` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/55`

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
    "stove0-target-conformance": "stove0_target_support.conformance:main",
    "stove0-target-schemas": "stove0_target_support.schemas:main"
  },
  "dependencies": [
    "http-api-contracts",
    "riverhog-client",
    "riverhog-protocol",
    "stove0-target-client",
    "stove0-target-protocol"
  ],
  "distribution": "stove0-target-support",
  "optional_dependencies": {},
  "path": "reference/stove0/packages/target-support",
  "role": "reusable_library"
}
```
