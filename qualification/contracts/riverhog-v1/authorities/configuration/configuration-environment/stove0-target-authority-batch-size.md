# STOVE0_TARGET_AUTHORITY_BATCH_SIZE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-target-authority-batch-size:101537ae1a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-a5bdad2233"></a>
| Field | Shape |
|---|---|
| <a id="s-ee93841a02"></a>`consumers` | ["stove0-server"] |
| <a id="s-4ca352f816"></a>`name` | "STOVE0_TARGET_AUTHORITY_BATCH_SIZE" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="STOVE0_TARGET_AUTHORITY_BATCH_SIZE"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_TARGET_AUTHORITY_BATCH_SIZE](#s-a5bdad2233) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-ef0026eb6f"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-7de8d60c5e"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:STOVE0_TARGET_AUTHORITY_BATCH_SIZE](../../../evidence/sources.md#src-1792d6b9fd) — `configuration-environment:STOVE0_TARGET_AUTHORITY_BATCH_SIZE`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/112`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1fed65789d3020d9b4b7c7a28e1526dd6ca619912787744890a162ccddb522ef -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_TARGET_AUTHORITY_BATCH_SIZE"
}
```
