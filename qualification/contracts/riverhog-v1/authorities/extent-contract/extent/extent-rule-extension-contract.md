# Extent rule: extension contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: extent:extent-contract:extent-rule-extension-contract:432e5bb05c -->

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
| `authority` | "the independently versioned extension contract" |
| `core_semantic_maximum` | null |
| `policy` | "extension_owned" |

## Governing policies

- `extent-rule/extension-contract/v1`

## Evidence

### Qualification

- `make contract-freeze`
- `make operation-qualification`

### Executable sources

- `extent:extent-contract` — `scripts/extent_contract.py::extent_projection`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/extents/rules/extension-contract~1v1`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: efca5346e716188b9a70547266ccd8626625735fffbb778123b478aa23865ec2 -->

```json
{
  "authority": "the independently versioned extension contract",
  "core_semantic_maximum": null,
  "policy": "extension_owned"
}
```
