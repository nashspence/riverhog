# STOVE0_CAPABILITY_TTL_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-capability-ttl-seconds:741e77c514 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-47fb606ae53d"></a>
| Field | Shape |
|---|---|
| <a id="s-c114306576ac"></a>`consumers` | ["stove0-server"] |
| <a id="s-e3159c3eeba5"></a>`name` | "STOVE0_CAPABILITY_TTL_SECONDS" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

Shared facts for every subject below: configuration="STOVE0_CAPABILITY_TTL_SECONDS"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_CAPABILITY_TTL_SECONDS](#s-47fb606ae53d) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-ac57c67feef2"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)
- <a id="pa-7541d670c6db"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:STOVE0_CAPABILITY_TTL_SECONDS](../../../evidence/sources.md#src-8bcfbbd2c374) — `configuration-environment:STOVE0_CAPABILITY_TTL_SECONDS`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/84`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e06d8848047d0b25589b1e76138cae20a380d7fb10f857a4312d2e347508e6de -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_CAPABILITY_TTL_SECONDS"
}
```
