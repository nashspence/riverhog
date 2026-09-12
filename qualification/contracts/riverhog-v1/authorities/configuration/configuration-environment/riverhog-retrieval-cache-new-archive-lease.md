# RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-retrieval-cache-new-archive-lease:e0ed23ec3c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-749bd6e141"></a>
| Field | Shape |
|---|---|
| <a id="s-032458916b"></a>`consumers` | ["riverhog-server"] |
| <a id="s-94ad8621b7"></a>`name` | "RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE](#s-749bd6e141) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-341efb4421"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-d4af8f42b8"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE](../../../evidence/sources.md#src-9737015df1) — `configuration-environment:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/59`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 61c358c206f102537a86cd0c5f4950dbffb7dac1b575524a138ea66645c9205d -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE"
}
```
