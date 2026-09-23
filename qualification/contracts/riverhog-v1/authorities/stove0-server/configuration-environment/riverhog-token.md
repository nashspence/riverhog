# RIVERHOG_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:riverhog-token:12382fc1af -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-b422e22e20"></a>

| Field | Value |
|---|---|
| <a id="s-96e799f355"></a>`consumers` | `["stove0-server"]` |
| <a id="s-0d52fe293f"></a>`default_expressions` | `["''"]` |
| <a id="s-8ba4ca9e6b"></a>`id` | `"stove0-server:environment:RIVERHOG_TOKEN"` |
| <a id="s-d22473b377"></a>`input_shape` | `"environment-string"` |
| <a id="s-9d56bbd460"></a>`name` | `"RIVERHOG_TOKEN"` |
| <a id="s-03340cb28d"></a>`owner` | `"stove0-server"` |

## Governing policies

- <a id="pa-a896e99fb4"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:RIVERHOG_TOKEN](../../../evidence/sources/authorities.md#src-a2d6243ee8) — [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py::\_secret](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py) | `values.get(name, '')` |

### Machine authority

- `/external_contract/configuration_environment/226`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1974c662db271a854ecfdd5d2915ab2e82bd454e4ec2e8514f90dabf3325d224 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-server:environment:RIVERHOG_TOKEN",
  "input_shape": "environment-string",
  "name": "RIVERHOG_TOKEN",
  "owner": "stove0-server"
}
```

</details>
