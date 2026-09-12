# Extent rule: route progression

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: extent:extent-contract:extent-rule-route-progression:528b079273 -->

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
| `authority` | "the route-owned x-riverhog-read-collection declaration" |
| `completion` | "the owning progression contract" |
| `policy` | "segmented_no_total_max" |

## Governing policies

- `extent-rule/route-progression/v1`

## Evidence

### Qualification

- `make contract-freeze`
- `make operation-qualification`

### Executable sources

- `extent:extent-contract` — `scripts/extent_contract.py::extent_projection`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/extents/rules/route-progression~1v1`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 524dea7a4d43815099baefe219d38866fbdfabfb1e615203e0c41da3cc11fca0 -->

```json
{
  "authority": "the route-owned x-riverhog-read-collection declaration",
  "completion": "the owning progression contract",
  "policy": "segmented_no_total_max"
}
```
