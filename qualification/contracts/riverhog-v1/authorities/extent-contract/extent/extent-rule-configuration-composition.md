# Extent rule: configuration composition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: extent:extent-contract:extent-rule-configuration-composition:64e89d3c00 -->

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
| `authority` | "the owning validated deployment configuration document" |
| `declared_operational_maximum` | null |
| `hidden_maximum` | "forbidden" |
| `policy` | "operational_policy" |
| `semantic_maximum` | null |
| `silent_truncation` | "forbidden" |

## Governing policies

- `extent-rule/configuration-composition/v1`

## Evidence

### Qualification

- `make contract-freeze`
- `make operation-qualification`

### Executable sources

- `extent:extent-contract` — `scripts/extent_contract.py::extent_projection`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/extents/rules/configuration-composition~1v1`

### Exact owned JSON

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
