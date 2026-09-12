# RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-retrieval-restore-poll-interval:91748b2db9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-a4749ac7f3"></a>
| Field | Shape |
|---|---|
| <a id="s-a43b0da62e"></a>`consumers` | ["riverhog-server"] |
| <a id="s-45655e8c1b"></a>`name` | "RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL](#s-a4749ac7f3) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-ba9fdb451e"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-a0b5a6c0c6"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL](../../../evidence/sources.md#src-f295b011d5) — `configuration-environment:RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/73`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5b3f6d86bef092473a85eddd3935cbb8eaf5dca8f7fc2c650c8eb81f0d3bf5be -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL"
}
```
