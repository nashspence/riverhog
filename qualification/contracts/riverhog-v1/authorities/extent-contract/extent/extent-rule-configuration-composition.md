# Extent rule: configuration composition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: extent:extent-contract:extent-rule-configuration-composition:64e89d3c00 -->

| Audit field | Value |
|---|---|
| Authority | `extent-contract` |
| Interface | `extent` |
| Family | `rules` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/extents/rules/configuration-composition~1v1`

## Effective policies

- `extent-rule/configuration-composition/v1`

## Executable sources and proof

- `extent:extent-contract` — `scripts/extent_contract.py::extent_projection`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make contract-freeze`
- Proof: `make operation-qualification`

## Contract summary

| Field | Shape |
|---|---|
| `authority` | "the owning validated deployment configuration document" |
| `declared_operational_maximum` | null |
| `hidden_maximum` | "forbidden" |
| `policy` | "operational_policy" |
| `semantic_maximum` | null |
| `silent_truncation` | "forbidden" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3ba1d5cdf00d6737eee66c195f42c64dcb0914af7bd8fd8bc1bbc7102b560420 -->

```json
{
  "authority": "the owning validated deployment configuration document",
  "declared_operational_maximum": null,
  "hidden_maximum": "forbidden",
  "policy": "operational_policy",
  "semantic_maximum": null,
  "silent_truncation": "forbidden"
}
```
