# RIVERHOG_PACK_SOURCE_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-pack-source-bytes:632df99fd2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-add94be491"></a>
| Field | Shape |
|---|---|
| <a id="s-d2bd3c7959"></a>`consumers` | ["riverhog-server"] |
| <a id="s-66e2078736"></a>`name` | "RIVERHOG_PACK_SOURCE_BYTES" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_PACK_SOURCE_BYTES"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_PACK_SOURCE_BYTES](#s-add94be491) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-96035b5644"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-36ee996728"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_PACK_SOURCE_BYTES](../../../evidence/sources.md#src-a27d020991) — `configuration-environment:RIVERHOG_PACK_SOURCE_BYTES`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/54`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 41df8a0bece104803266389345f0257dd367d5829404c924fa3e26aa25dcf6aa -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_PACK_SOURCE_BYTES"
}
```
