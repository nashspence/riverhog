# stove0-review-sampler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-review-sampler-protocol:stove0-review-sampler:8e98df71df -->

| Audit field | Value |
|---|---|
| Authority | `stove0-review-sampler-protocol` |
| Interface | `boundary` |
| Family | `process-extensions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/process_extensions/2`

## Effective policies

- `boundary/frozen-authority/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`
- Proof: `make release-check`
- Proof: `make build`

## Contract summary

- `protocols`: ["stove0-review-sampler/v1"]

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4841891cc9b850de788312cd1fbd692a1788dab12d2efdc77ee311a5379ecec0 -->

```json
{
  "binding": "http",
  "binding_support": "stove0-review-sampler-support",
  "binding_support_role": "reference_component",
  "contract_owner": "stove0-review-sampler-protocol",
  "contract_owner_role": "reference_component",
  "name": "stove0-review-sampler",
  "protocols": [
    "stove0-review-sampler/v1"
  ],
  "providers": [
    {
      "distribution": "stove0-nvenc-av1-opus-review-sampler",
      "images": [
        "stove0-nvenc-av1-opus-target"
      ]
    },
    {
      "distribution": "stove0-opus-review-sampler",
      "images": [
        "stove0-opus-target"
      ]
    }
  ],
  "schema_bundle_format": "stove0-review-sampler-schema-bundle/v1"
}
```
