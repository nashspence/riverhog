# RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-catalog-sync-history-retention:a2e009f651 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-2d908ff8df4d"></a>
| Field | Shape |
|---|---|
| <a id="s-c799298a709a"></a>`consumers` | ["riverhog-server"] |
| <a id="s-c530fa2f45f7"></a>`name` | "RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

Shared facts for every subject below: configuration="RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION](#s-2d908ff8df4d) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-bbcee626350b"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)
- <a id="pa-9a25ce0311d5"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION](../../../evidence/sources.md#src-ac7f42ab580b) — `configuration-environment:RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/30`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9aaaff97692b72b9f7ba563745aed4ea9c0c652fdc8a135ff11e308e46bd2486 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION"
}
```
