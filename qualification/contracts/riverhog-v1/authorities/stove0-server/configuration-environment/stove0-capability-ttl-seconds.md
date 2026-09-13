# STOVE0_CAPABILITY_TTL_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-capability-ttl-seconds:cb22c7236d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-12475197c9) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-7784b989ae"></a>
| Field | Shape |
|---|---|
| <a id="s-f8db0d8e09"></a>`consumers` | ["stove0-server"] |
| <a id="s-bd0e429538"></a>`default_expressions` | ["str(default)"] |
| <a id="s-dce2eba6ce"></a>`id` | "stove0-server:environment:STOVE0_CAPABILITY_TTL_SECONDS" |
| <a id="s-0a01ffc5ac"></a>`input_shape` | "environment-string" |
| <a id="s-acebbf3768"></a>`name` | "STOVE0_CAPABILITY_TTL_SECONDS" |
| <a id="s-4782fa51bf"></a>`owner` | "stove0-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="STOVE0_CAPABILITY_TTL_SECONDS"; consumers=["stove0-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_CAPABILITY_TTL_SECONDS](#s-7784b989ae) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-26ac495a96"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-96e29269c0"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_CAPABILITY_TTL_SECONDS](../../../evidence/sources.md#src-0e9a09a14d) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `values.get(name, str(default))` |

### Machine authority

- `/external_contract/configuration_environment/234`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 706b9bb54445b726cbbb4b144f73554a324f722bdd57ebb366a6365d7d3f8d31 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "str(default)"
  ],
  "id": "stove0-server:environment:STOVE0_CAPABILITY_TTL_SECONDS",
  "input_shape": "environment-string",
  "name": "STOVE0_CAPABILITY_TTL_SECONDS",
  "owner": "stove0-server"
}
```
