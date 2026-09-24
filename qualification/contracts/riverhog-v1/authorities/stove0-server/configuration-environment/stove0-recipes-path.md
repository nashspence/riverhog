# STOVE0_RECIPES_PATH

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-recipes-path:3b8d48dc02 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-704a0f2188"></a>

| Field | Value |
|---|---|
| <a id="s-a976fdbf26"></a>`consumers` | `["stove0-server"]` |
| <a id="s-c44939a0ce"></a>`default_expressions` | `["''"]` |
| <a id="s-cf1d980c19"></a>`id` | `"stove0-server:environment:STOVE0_RECIPES_PATH"` |
| <a id="s-80e58005d1"></a>`input_shape` | `"environment-string"` |
| <a id="s-6ced135eba"></a>`name` | `"STOVE0_RECIPES_PATH"` |
| <a id="s-8c5a0f1546"></a>`owner` | `"stove0-server"` |

## Governing policies

- <a id="pa-9f84f6fe10"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

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

- `/external_contract/configuration_environment/242`

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
