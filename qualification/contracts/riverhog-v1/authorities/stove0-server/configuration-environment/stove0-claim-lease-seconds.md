# STOVE0_CLAIM_LEASE_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-claim-lease-seconds:dce09984ab -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-2832b8e991"></a>
| Field | Shape |
|---|---|
| <a id="s-8fb4f3e4c3"></a>`consumers` | ["stove0-server"] |
| <a id="s-f8450a61d2"></a>`default_expressions` | ["str(default)"] |
| <a id="s-77881b1fba"></a>`id` | "stove0-server:environment:STOVE0_CLAIM_LEASE_SECONDS" |
| <a id="s-fc0a93c82e"></a>`input_shape` | "environment-string" |
| <a id="s-2656b48bb7"></a>`name` | "STOVE0_CLAIM_LEASE_SECONDS" |
| <a id="s-25093ba074"></a>`owner` | "stove0-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="STOVE0_CLAIM_LEASE_SECONDS"; consumers=["stove0-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_CLAIM_LEASE_SECONDS](#s-2832b8e991) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-a878794b4d"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-b0c303a059"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_CLAIM_LEASE_SECONDS](../../../evidence/sources.md#src-1997f656ba) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `values.get(name, str(default))` |

### Machine authority

- `/external_contract/configuration_environment/235`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9fa8c9ef09d45cdd333b1b633188e3c9661a3bef2df7a27988955bad9e3237a5 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "str(default)"
  ],
  "id": "stove0-server:environment:STOVE0_CLAIM_LEASE_SECONDS",
  "input_shape": "environment-string",
  "name": "STOVE0_CLAIM_LEASE_SECONDS",
  "owner": "stove0-server"
}
```
