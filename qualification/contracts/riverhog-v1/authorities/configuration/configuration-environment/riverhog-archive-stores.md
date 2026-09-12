# RIVERHOG_ARCHIVE_STORES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-archive-stores:c1c79eaf2d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-ef80127a53"></a>
| Field | Shape |
|---|---|
| <a id="s-bf7f86f2e0"></a>`consumers` | ["riverhog-server"] |
| <a id="s-53140a7a23"></a>`name` | "RIVERHOG_ARCHIVE_STORES" |

## Governing policies

- <a id="pa-5220b4c188"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_ARCHIVE_STORES](../../../evidence/sources.md#src-9f73907a83) — `configuration-environment:RIVERHOG_ARCHIVE_STORES`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/18`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0b410af2f6f9410fce9553e91d7fbbc78028ce6c74fc117fa0740e0b47985358 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_ARCHIVE_STORES"
}
```
