# STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-target-terminal-state-retention-seconds:e13cf9bd3c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-7c4bc7f8a3f0"></a>
| Field | Shape |
|---|---|
| <a id="s-de3bbf83d5b3"></a>`consumers` | ["stove0-target-support"] |
| <a id="s-033e682cb844"></a>`name` | "STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

Shared facts for every subject below: configuration="STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS](#s-7c4bc7f8a3f0) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-12d7cc7f60dd"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)
- <a id="pa-316a4f2a24bc"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS](../../../evidence/sources.md#src-4dced914a586) — `configuration-environment:STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/116`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c1476afe5be8b5a91c3e6cc400580431a0b534ff8a2e996fbfa0185a61a01bbf -->

```json
{
  "consumers": [
    "stove0-target-support"
  ],
  "name": "STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS"
}
```
