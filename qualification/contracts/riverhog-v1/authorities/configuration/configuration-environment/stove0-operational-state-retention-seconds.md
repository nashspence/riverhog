# STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-operational-state-retention-seconds:6d2f2e0943 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-8af4063a2bc5"></a>
| Field | Shape |
|---|---|
| <a id="s-780fea1d0c07"></a>`consumers` | ["stove0-server"] |
| <a id="s-9feb87a0c4b7"></a>`name` | "STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

Shared facts for every subject below: configuration="STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS](#s-8af4063a2bc5) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-18a1c4c6908f"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)
- <a id="pa-f50feeac9bbf"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS](../../../evidence/sources.md#src-218c436ac137) — `configuration-environment:STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/108`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aae2ee69833a90a419af98af5bd6c1316ed175901beece77bceed74dc03d6522 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS"
}
```
