# STOVE0_OBSERVERS_JSON

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-observers-json:1e015b9e14 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-19236d188c"></a>

| Field | Value |
|---|---|
| <a id="s-3e86e0e209"></a>`consumers` | `["stove0-server"]` |
| <a id="s-77a8d1d65a"></a>`default_expressions` | `["'{}'"]` |
| <a id="s-7fd78e78b0"></a>`id` | `"stove0-server:environment:STOVE0_OBSERVERS_JSON"` |
| <a id="s-8081c6a9e7"></a>`input_shape` | `"environment-string"` |
| <a id="s-4ebbcd1f29"></a>`name` | `"STOVE0_OBSERVERS_JSON"` |
| <a id="s-ae9d0d044f"></a>`owner` | `"stove0-server"` |

## Governing policies

- <a id="pa-edbbaa71c9"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_OBSERVERS_JSON](../../../evidence/sources/authorities.md#src-5e770d1e0f) — [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py::\_registrations](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py) | `values.get(name, '{}')` |

### Machine authority

- `/external_contract/configuration_environment/240`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0e3ef4e08911ff6cb2820f17986e7dfb33d01e02e059590267b12187de9065ff -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "'{}'"
  ],
  "id": "stove0-server:environment:STOVE0_OBSERVERS_JSON",
  "input_shape": "environment-string",
  "name": "STOVE0_OBSERVERS_JSON",
  "owner": "stove0-server"
}
```

</details>
