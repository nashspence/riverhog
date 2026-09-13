# RIVERHOG_PACK_SOURCE_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-pack-source-bytes:f66dc30071 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-90cd77baf7"></a>
| Field | Shape |
|---|---|
| <a id="s-bc48d55b1e"></a>`consumers` | ["riverhog-server"] |
| <a id="s-8f0011fd5b"></a>`default_expressions` | ["unset"] |
| <a id="s-48764ad0e2"></a>`id` | "riverhog-server:environment:RIVERHOG_PACK_SOURCE_BYTES" |
| <a id="s-9d81873663"></a>`input_shape` | "environment-string" |
| <a id="s-35c021851e"></a>`name` | "RIVERHOG_PACK_SOURCE_BYTES" |
| <a id="s-5bf462d167"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_PACK_SOURCE_BYTES"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_PACK_SOURCE_BYTES](#s-90cd77baf7) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-fc2bd1b318"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-0bb4819334"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_PACK_SOURCE_BYTES](../../../evidence/sources.md#src-b2ca118143) — `riverhog/src/riverhog_core/collection_plan.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/collection_plan.py` | `values.get(name)` |

### Machine authority

- `/external_contract/configuration_environment/66`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cb1fa38c724be242525eb3c7203665b1c1220d7e7af39cd8a26752e7e3205782 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-server:environment:RIVERHOG_PACK_SOURCE_BYTES",
  "input_shape": "environment-string",
  "name": "RIVERHOG_PACK_SOURCE_BYTES",
  "owner": "riverhog-server"
}
```
