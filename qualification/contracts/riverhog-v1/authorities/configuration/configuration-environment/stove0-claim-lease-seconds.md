# STOVE0_CLAIM_LEASE_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-claim-lease-seconds:654731b407 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-8a57472b3d"></a>
| Field | Shape |
|---|---|
| <a id="s-8b87502075"></a>`consumers` | ["stove0-server"] |
| <a id="s-ebf779a82f"></a>`name` | "STOVE0_CLAIM_LEASE_SECONDS" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="STOVE0_CLAIM_LEASE_SECONDS"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_CLAIM_LEASE_SECONDS](#s-8a57472b3d) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-b36ab49bbe"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-b063e912dd"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:STOVE0_CLAIM_LEASE_SECONDS](../../../evidence/sources.md#src-a964a6d069) — `configuration-environment:STOVE0_CLAIM_LEASE_SECONDS`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/85`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0977e9c8ff7b9981ace64b4e1d9cac4c93d4e61761343326e75834c1f8fd9eca -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_CLAIM_LEASE_SECONDS"
}
```
