# RIVERHOG_PACK_SOURCE_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-pack-source-bytes:e6eb1b11e9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-0630da4c2b"></a>

| Field | Value |
|---|---|
| <a id="s-02ae451182"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-9b5f9f836f"></a>`default_expressions` | `["unset"]` |
| <a id="s-8acc528e3b"></a>`id` | `"riverhog-server:environment:RIVERHOG_PACK_SOURCE_BYTES"` |
| <a id="s-658134addc"></a>`input_shape` | `"environment-string"` |
| <a id="s-39b689166d"></a>`name` | `"RIVERHOG_PACK_SOURCE_BYTES"` |
| <a id="s-5bd49751e0"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_PACK_SOURCE_BYTES"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_PACK_SOURCE_BYTES](#s-0630da4c2b) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-a3d601734b"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-83e487a1d1"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_PACK_SOURCE_BYTES](../../../evidence/sources/authorities.md#src-b2ca118143) — [riverhog/src/riverhog\_core/collection\_plan.py::\_env\_bytes](../../../../../../riverhog/src/riverhog_core/collection_plan.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/collection\_plan.py](../../../../../../riverhog/src/riverhog_core/collection_plan.py) | `values.get(name)` |

### Machine authority

- `/external_contract/configuration_environment/199`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
