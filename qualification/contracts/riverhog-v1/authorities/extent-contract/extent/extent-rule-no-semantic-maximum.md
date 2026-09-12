# Extent rule: no semantic maximum

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: extent:extent-contract:extent-rule-no-semantic-maximum:dbf6e2bff5 -->

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
| `authority` | "the owning schema's deliberate absence of a semantic maximum" |
| `declared_operational_maximum` | null |
| `future_capacity_behavior` | "explicit-configured-reject-defer-or-throttle" |
| `hidden_maximum` | "forbidden" |
| `policy` | "operational_policy" |
| `semantic_maximum` | null |
| `silent_truncation` | "forbidden" |

## Governing policies

- `extent-rule/no-semantic-maximum/v1`

## Evidence

### Qualification

- `make contract-freeze`
- `make operation-qualification`

### Executable sources

- `extent:extent-contract` — `scripts/extent_contract.py::extent_projection`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/extents/rules/no-semantic-maximum~1v1`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 74ed836834648a8834851fe68e382be2989d41657f840c93889a7817b65f8f32 -->

```json
{
  "authority": "the owning schema's deliberate absence of a semantic maximum",
  "declared_operational_maximum": null,
  "future_capacity_behavior": "explicit-configured-reject-defer-or-throttle",
  "hidden_maximum": "forbidden",
  "policy": "operational_policy",
  "semantic_maximum": null,
  "silent_truncation": "forbidden"
}
```
