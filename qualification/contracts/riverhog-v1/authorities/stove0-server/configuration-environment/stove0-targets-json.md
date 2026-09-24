# STOVE0_TARGETS_JSON

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-targets-json:1196b67571 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-511d2ba0da"></a>

| Field | Value |
|---|---|
| <a id="s-95f3a14158"></a>`consumers` | `["stove0-server"]` |
| <a id="s-3f7daf7f5f"></a>`default_expressions` | `["'{}'"]` |
| <a id="s-eefa9e99c9"></a>`id` | `"stove0-server:environment:STOVE0_TARGETS_JSON"` |
| <a id="s-168f3b71fe"></a>`input_shape` | `"environment-string"` |
| <a id="s-f58b650a7a"></a>`name` | `"STOVE0_TARGETS_JSON"` |
| <a id="s-13d61e494d"></a>`owner` | `"stove0-server"` |

## Governing policies

- <a id="pa-d60d576305"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_TARGETS_JSON](../../../evidence/sources/authorities.md#src-05b1eb365e) — [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py::\_registrations](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py) | `values.get(name, '{}')` |

### Machine authority

- `/external_contract/configuration_environment/244`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5078f653c930d933f6878a54ac017de18603b98b5d663fa18e4b13c9ec60dd6d -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "'{}'"
  ],
  "id": "stove0-server:environment:STOVE0_TARGETS_JSON",
  "input_shape": "environment-string",
  "name": "STOVE0_TARGETS_JSON",
  "owner": "stove0-server"
}
```

</details>
