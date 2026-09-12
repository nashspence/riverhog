# RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-catalog-sync-page-size-max:0c41edb266 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-c4b672867c"></a>
| Field | Shape |
|---|---|
| <a id="s-396c43f461"></a>`consumers` | ["riverhog-server"] |
| <a id="s-019de684d7"></a>`name` | "RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX](#s-c4b672867c) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-b7d3b7a861"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-3a338fcaf4"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX](../../../evidence/sources.md#src-4be5bf418d) — `configuration-environment:RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/31`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d18ef7b22fb469f9692f9471ab509eb4ea74100c0323a14de60bd9697c017672 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX"
}
```
