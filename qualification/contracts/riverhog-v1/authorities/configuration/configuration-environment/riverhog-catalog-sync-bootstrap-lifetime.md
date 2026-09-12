# RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-catalog-sync-bootstrap-lifetime:33cf569c58 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-d99f18df3bd2"></a>
| Field | Shape |
|---|---|
| <a id="s-68e07c8d4079"></a>`consumers` | ["riverhog-server"] |
| <a id="s-7a3900970064"></a>`name` | "RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME" |

## Governing policies

- <a id="pa-ab7b5e43aeb6"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME](../../../evidence/sources.md#src-ec14a484889b) — `configuration-environment:RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/27`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7861381bd46ac3942751c1911dddf1719991b529a02d6fbe21fdaa5d72b7e4ed -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME"
}
```
