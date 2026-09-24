# STOVE0_DECLARED_WORKSPACE_PROTECTION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-declared-workspace-protection:83edd2a012 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-d19efb74d1"></a>

| Field | Value |
|---|---|
| <a id="s-decf158927"></a>`consumers` | `["stove0-server"]` |
| <a id="s-93cce4292e"></a>`default_expressions` | `["''"]` |
| <a id="s-7d623ddfb5"></a>`id` | `"stove0-server:environment:STOVE0_DECLARED_WORKSPACE_PROTECTION"` |
| <a id="s-a92824bed5"></a>`input_shape` | `"environment-string"` |
| <a id="s-c8688fc9d6"></a>`name` | `"STOVE0_DECLARED_WORKSPACE_PROTECTION"` |
| <a id="s-511e086952"></a>`owner` | `"stove0-server"` |

## Governing policies

- <a id="pa-d66b47297a"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_DECLARED_WORKSPACE_PROTECTION](../../../evidence/sources/authorities.md#src-3fc5c84298) — [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py::\_required](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py) | `values.get(name, '')` |

### Machine authority

- `/external_contract/configuration_environment/237`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 82e2c0ab85514967f80bd6905ce4877ee9b06aa5c179b42eed197f5985fad5da -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-server:environment:STOVE0_DECLARED_WORKSPACE_PROTECTION",
  "input_shape": "environment-string",
  "name": "STOVE0_DECLARED_WORKSPACE_PROTECTION",
  "owner": "stove0-server"
}
```

</details>
