# STOVE0_RECIPES_PATH

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-recipes-path:588cf753f1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-f1434e7972"></a>

| Field | Value |
|---|---|
| <a id="s-8b1bac247a"></a>`consumers` | `["stove0-server"]` |
| <a id="s-7885b7d9a8"></a>`default_expressions` | `["''"]` |
| <a id="s-cd0ba82178"></a>`id` | `"stove0-server:environment:STOVE0_RECIPES_PATH"` |
| <a id="s-cf63d91846"></a>`input_shape` | `"environment-string"` |
| <a id="s-3e0815658c"></a>`name` | `"STOVE0_RECIPES_PATH"` |
| <a id="s-4651ce2447"></a>`owner` | `"stove0-server"` |

## Governing policies

- <a id="pa-0475724ef5"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_RECIPES_PATH](../../../evidence/sources/authorities.md#src-afbf7c2b57) — [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py::\_required](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py) | `values.get(name, '')` |

### Machine authority

- `/external_contract/configuration_environment/241`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0ef73a38381eb7f4934c4684b41ed63a5c4b67fd1d799d38c980d1c55f625651 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-server:environment:STOVE0_RECIPES_PATH",
  "input_shape": "environment-string",
  "name": "STOVE0_RECIPES_PATH",
  "owner": "stove0-server"
}
```

</details>
