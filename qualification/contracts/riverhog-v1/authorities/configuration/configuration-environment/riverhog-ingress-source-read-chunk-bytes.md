# RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-ingress-source-read-chunk-bytes:51b1621e81 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-71aef6f25d5d"></a>
| Field | Shape |
|---|---|
| <a id="s-a3df3866fb66"></a>`consumers` | ["riverhog-server"] |
| <a id="s-8019296a335b"></a>`name` | "RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

Shared facts for every subject below: configuration="RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES](#s-71aef6f25d5d) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-e7fcba652cdc"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)
- <a id="pa-0c3429630c64"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES](../../../evidence/sources.md#src-2e9902b11ab1) — `configuration-environment:RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/50`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c21bb8504f997adbdfad629a4f8bc4b89a332afde126b83e673e9fdac7872d48 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES"
}
```
