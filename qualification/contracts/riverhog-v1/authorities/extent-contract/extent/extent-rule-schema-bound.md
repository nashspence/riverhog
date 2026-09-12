# Extent rule: schema bound

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: extent:extent-contract:extent-rule-schema-bound:7113c46925 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [extent-contract](../index.md) |
| Interface | [extent](index.md) |
| Family | [rules](index.md#f-b9df1e5029) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-6b20b7da6a"></a>
| Field | Shape |
|---|---|
| <a id="s-5a12cda572"></a>`authority` | "the projected JSON Schema constraint" |
| <a id="s-87b468c17c"></a>`exceeded` | "schema-validation-error" |
| <a id="s-8477a70770"></a>`policy` | "fixed-or-contract-max" |
| <a id="s-0aa687ca3d"></a>`requirement` | "a non-fixed set maximum carries an owning reason declaration" |

## Governing policies

- <a id="pa-b74b0aafeb"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make contract-freeze](../../../evidence/sources.md#q-8c74349e71)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [extent:extent-contract](../../../evidence/sources.md#src-5ac94d0a12) — `scripts/extent_contract.py::extent_projection`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/extents/rules/schema-bound~1v1`

### Exact owned JSON

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
