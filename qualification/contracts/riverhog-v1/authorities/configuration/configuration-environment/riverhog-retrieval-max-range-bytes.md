# RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-retrieval-max-range-bytes:8e0d99a9a2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-a44ff15aa702"></a>
| Field | Shape |
|---|---|
| <a id="s-cbfaedd8f7b0"></a>`consumers` | ["riverhog-server"] |
| <a id="s-394d41cd98d1"></a>`name` | "RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES](#s-a44ff15aa702) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-2aabcf6f7050"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)
- <a id="pa-51ae05a63bd3"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES](../../../evidence/sources.md#src-a99d583099d6) — `configuration-environment:RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/67`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5f1027cd8a3a0fc7296339cc24833c551a2dc45952910131df43c0cf062cd6a5 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES"
}
```
