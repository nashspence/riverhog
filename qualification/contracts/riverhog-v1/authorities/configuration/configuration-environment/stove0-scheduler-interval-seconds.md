# STOVE0_SCHEDULER_INTERVAL_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-scheduler-interval-seconds:7bc6463af2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-6cd174187017"></a>
| Field | Shape |
|---|---|
| <a id="s-f89c1c30488f"></a>`consumers` | ["stove0-server"] |
| <a id="s-7027ddbe7117"></a>`name` | "STOVE0_SCHEDULER_INTERVAL_SECONDS" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

Shared facts for every subject below: configuration="STOVE0_SCHEDULER_INTERVAL_SECONDS"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_SCHEDULER_INTERVAL_SECONDS](#s-6cd174187017) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-93d24e4b0d78"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)
- <a id="pa-23b58dd980d8"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:STOVE0_SCHEDULER_INTERVAL_SECONDS](../../../evidence/sources.md#src-1f626435d140) — `configuration-environment:STOVE0_SCHEDULER_INTERVAL_SECONDS`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/110`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0339ff465a59246f9e7bcfee50ca5d24ba85c37ec2e72640e4758a98590f0b7f -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_SCHEDULER_INTERVAL_SECONDS"
}
```
