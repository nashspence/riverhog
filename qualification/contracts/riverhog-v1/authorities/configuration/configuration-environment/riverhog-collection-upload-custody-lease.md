# RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-collection-upload-custody-lease:2d14e7419c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-a9fb2eff34"></a>
| Field | Shape |
|---|---|
| <a id="s-c00038c38f"></a>`consumers` | ["riverhog-server"] |
| <a id="s-caff9576d7"></a>`name` | "RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE](#s-a9fb2eff34) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-60daf99974"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-f16c971941"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE](../../../evidence/sources.md#src-993ad537df) — `configuration-environment:RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/32`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 449cd7855ac1a721fa4bfcbff9f7ba902aa239ddfc68a8ed1ff06a371acd3fb0 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE"
}
```
