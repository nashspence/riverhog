# STOVE0_RECIPES_PATH

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-recipes-path:dbcc77d2f5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-19236d188c"></a>
| Field | Shape |
|---|---|
| <a id="s-3e86e0e209"></a>`consumers` | ["stove0-server"] |
| <a id="s-77a8d1d65a"></a>`default_expressions` | ["''"] |
| <a id="s-7fd78e78b0"></a>`id` | "stove0-server:environment:STOVE0_RECIPES_PATH" |
| <a id="s-8081c6a9e7"></a>`input_shape` | "environment-string" |
| <a id="s-4ebbcd1f29"></a>`name` | "STOVE0_RECIPES_PATH" |
| <a id="s-ae9d0d044f"></a>`owner` | "stove0-server" |

## Governing policies

- <a id="pa-06526da700"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_RECIPES_PATH](../../../evidence/sources.md#src-afbf7c2b57) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `values.get(name, '')` |

### Machine authority

- `/external_contract/configuration_environment/240`

### Exact owned JSON

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
