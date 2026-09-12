# stove0-opus-target component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-opus-target:stove0-opus-target-component-boundary:1eafd2ccdf -->

| Audit field | Value |
|---|---|
| Authority | `stove0-opus-target` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/61`

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
    "stove0-opus-target": "stove0_opus_target.app:target_main"
  },
  "dependencies": [
    "http-api-contracts",
    "riverhog-client",
    "riverhog-protocol",
    "stove0-media-archive-target-contracts",
    "stove0-media-archive-target-support",
    "stove0-protocol",
    "stove0-target-support"
  ],
  "distribution": "stove0-opus-target",
  "optional_dependencies": {},
  "path": "reference/stove0/targets/opus/target",
  "role": "reference_component"
}
```
