# RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-ingress-max-inflight-bytes:2b3d75f38d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-be3c1ab328"></a>
| Field | Shape |
|---|---|
| <a id="s-53a8b95257"></a>`consumers` | ["riverhog-server"] |
| <a id="s-e79d9952b9"></a>`name` | "RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES](#s-be3c1ab328) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-3970861215"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-c0a8d25e39"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES](../../../evidence/sources.md#src-5c35b8da98) — `configuration-environment:RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/49`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 571aee796161d24ff211ebd579669c4a7b667be882d2a0767cdce0fd4571a7b8 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES"
}
```
