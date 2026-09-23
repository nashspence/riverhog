# STOVE0_API_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-api-token:27d1df0d0f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-30c0913544"></a>

| Field | Value |
|---|---|
| <a id="s-d00e440158"></a>`consumers` | `["stove0-server"]` |
| <a id="s-665a0384a8"></a>`default_expressions` | `["''"]` |
| <a id="s-6b03089d6d"></a>`id` | `"stove0-server:environment:STOVE0_API_TOKEN"` |
| <a id="s-0c7781c780"></a>`input_shape` | `"environment-string"` |
| <a id="s-4fdea0c27a"></a>`name` | `"STOVE0_API_TOKEN"` |
| <a id="s-8ee07180a7"></a>`owner` | `"stove0-server"` |

## Governing policies

- <a id="pa-82758651ec"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_API_TOKEN](../../../evidence/sources/authorities.md#src-b54731a17b) — [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py::\_secret](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py) | `values.get(name, '')` |

### Machine authority

- `/external_contract/configuration_environment/229`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 66513233348bdbfc9093c7224ffea7fedf06b357880648ad42aef96544b97b15 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-server:environment:STOVE0_API_TOKEN",
  "input_shape": "environment-string",
  "name": "STOVE0_API_TOKEN",
  "owner": "stove0-server"
}
```

</details>
