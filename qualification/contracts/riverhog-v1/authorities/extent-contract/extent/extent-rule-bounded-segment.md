# Extent rule: bounded segment

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: extent:extent-contract:extent-rule-bounded-segment:6e947b7583 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `extent-contract` |
| Interface | `extent` |
| Family | `rules` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `authority` | "the owning schema's x-riverhog-extent declaration" |
| `completion` | "the owner-declared progression or repeated-work contract" |
| `exceeded` | "bounded-carrier-validation-error" |
| `policy` | "segmented_no_total_max" |
| `semantic_maximum` | null |

## Governing policies

- `extent-rule/bounded-segment/v1`

## Evidence

### Qualification

- `make contract-freeze`
- `make operation-qualification`

### Executable sources

- `extent:extent-contract` — `scripts/extent_contract.py::extent_projection`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/extents/rules/bounded-segment~1v1`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 42485f405b18cf5471144b12f8e6a7bd05c78bba9e4f67d614f9e5725612ec6e -->

```json
{
  "authority": "the owning schema's x-riverhog-extent declaration",
  "completion": "the owner-declared progression or repeated-work contract",
  "exceeded": "bounded-carrier-validation-error",
  "policy": "segmented_no_total_max",
  "semantic_maximum": null
}
```
