# Extent rule: schema bound

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: extent:extent-contract:extent-rule-schema-bound:7113c46925 -->

| Audit field | Value |
|---|---|
| Authority | `extent-contract` |
| Interface | `extent` |
| Family | `rules` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/extents/rules/schema-bound~1v1`

## Effective policies

- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `extent:extent-contract` — `scripts/extent_contract.py::extent_projection`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make contract-freeze`
- Proof: `make operation-qualification`

## Contract summary

| Field | Shape |
|---|---|
| `authority` | "the projected JSON Schema constraint" |
| `exceeded` | "schema-validation-error" |
| `policy` | "fixed-or-contract-max" |
| `requirement` | "a non-fixed set maximum carries an owning reason declaration" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 67a37a1700f335d04859cdd7551d06b87954e7eb258eadcfcec4e38f8d01a96d -->

```json
{
  "authority": "the projected JSON Schema constraint",
  "exceeded": "schema-validation-error",
  "policy": "fixed-or-contract-max",
  "requirement": "a non-fixed set maximum carries an owning reason declaration"
}
```
