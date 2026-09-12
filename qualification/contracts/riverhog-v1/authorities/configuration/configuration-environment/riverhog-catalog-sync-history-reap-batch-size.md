# RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-catalog-sync-history-reap-batch-size:31524c0fa6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-eb2021dc66"></a>
| Field | Shape |
|---|---|
| <a id="s-f72318c871"></a>`consumers` | ["riverhog-server"] |
| <a id="s-b4738a40ad"></a>`name` | "RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE](#s-eb2021dc66) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-c1c00cff5a"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-f90909a553"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE](../../../evidence/sources.md#src-8c377756e4) — `configuration-environment:RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/29`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cfc6df069ef892e89947f3a19cd47a71863e60a8b7246773f1f2725cc058e969 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE"
}
```
