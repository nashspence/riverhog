# Extent rule: configured capacity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: extent:extent-contract:extent-rule-configured-capacity:04e3baba41 -->

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
| `authority` | "the source-linked operator configuration field" |
| `capacity_behavior` | "explicit-reject-defer-or-throttle" |
| `policy` | "operational_policy" |
| `silent_truncation` | "forbidden" |

## Governing policies

- `extent-rule/configured-capacity/v1`

## Evidence

### Qualification

- `make contract-freeze`
- `make operation-qualification`

### Executable sources

- `extent:extent-contract` — `scripts/extent_contract.py::extent_projection`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/extents/rules/configured-capacity~1v1`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d22ade33fd6b9425ca71fc8bcf777549db5a17856ba18d64838679ec8d004742 -->

```json
{
  "authority": "the source-linked operator configuration field",
  "capacity_behavior": "explicit-reject-defer-or-throttle",
  "policy": "operational_policy",
  "silent_truncation": "forbidden"
}
```
