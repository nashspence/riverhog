# RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-catalog-sync-cursor-lifetime:60c467a308 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-d7a4e6e0e8"></a>
| Field | Shape |
|---|---|
| <a id="s-a5e7a20adb"></a>`consumers` | ["riverhog-server"] |
| <a id="s-495c4fd74d"></a>`name` | "RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME" |

## Governing policies

- <a id="pa-7809adc88b"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME](../../../evidence/sources.md#src-00e8bb321e) — `configuration-environment:RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/28`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 970506af52c3d45714af10200df6878d5e2c050b7d8e559ada0b0ebbab989055 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME"
}
```
