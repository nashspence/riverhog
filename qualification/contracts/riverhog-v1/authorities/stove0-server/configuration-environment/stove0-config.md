# STOVE0_CONFIG

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-config:859f53903b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-7a1e93aef1"></a>

| Field | Value |
|---|---|
| <a id="s-31f233c17f"></a>`consumers` | `["stove0-server"]` |
| <a id="s-4e6795016d"></a>`default_expressions` | `["''"]` |
| <a id="s-8b573f8c6b"></a>`id` | `"stove0-server:environment:STOVE0_CONFIG"` |
| <a id="s-8fde01c655"></a>`input_shape` | `"environment-string"` |
| <a id="s-be64f6ddb2"></a>`name` | `"STOVE0_CONFIG"` |
| <a id="s-8669d543f0"></a>`owner` | `"stove0-server"` |

## Governing policies

- <a id="pa-323bb3c1fe"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_CONFIG](../../../evidence/sources/authorities.md#src-5a22230d36) — [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py::database\_url\_from\_config](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py); [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py::load\_stove0\_config](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py) | `os.environ.get('STOVE0_CONFIG', '')` |
| parser | `stove0-server` | [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py) | `os.environ.get('STOVE0_CONFIG', '')` |

### Machine authority

- `/external_contract/configuration_environment/119`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ea95b813b0446ff648eeb8a5a58fa635a58225e7385a87a81b27915e34fd0ef0 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-server:environment:STOVE0_CONFIG",
  "input_shape": "environment-string",
  "name": "STOVE0_CONFIG",
  "owner": "stove0-server"
}
```

</details>
