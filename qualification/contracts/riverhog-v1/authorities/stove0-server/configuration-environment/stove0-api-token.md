# STOVE0_API_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-api-token:6496a5b572 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-f82c408db3"></a>

| Field | Value |
|---|---|
| <a id="s-92991d302e"></a>`consumers` | `["stove0-server"]` |
| <a id="s-e8c8558325"></a>`default_expressions` | `["''"]` |
| <a id="s-6686ac562c"></a>`id` | `"stove0-server:environment:STOVE0_API_TOKEN"` |
| <a id="s-a1d5fd2666"></a>`input_shape` | `"environment-string"` |
| <a id="s-dbdacef493"></a>`name` | `"STOVE0_API_TOKEN"` |
| <a id="s-41ea2c0785"></a>`owner` | `"stove0-server"` |

## Governing policies

- <a id="pa-7074ecd411"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

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

- `/external_contract/configuration_environment/228`

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
