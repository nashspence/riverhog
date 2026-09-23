# STOVE0_TARGET_CALLBACK_SIGNING_KEY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-target-callback-signing-key:9cbaffba16 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-dd749a07a4"></a>

| Field | Value |
|---|---|
| <a id="s-3e0dc801fc"></a>`consumers` | `["stove0-server"]` |
| <a id="s-52577a5285"></a>`default_expressions` | `["''"]` |
| <a id="s-7dbc7abda1"></a>`id` | `"stove0-server:environment:STOVE0_TARGET_CALLBACK_SIGNING_KEY"` |
| <a id="s-fa7497d808"></a>`input_shape` | `"environment-string"` |
| <a id="s-305a3525e1"></a>`name` | `"STOVE0_TARGET_CALLBACK_SIGNING_KEY"` |
| <a id="s-bcdbfe84df"></a>`owner` | `"stove0-server"` |

## Governing policies

- <a id="pa-b28058fbec"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_TARGET_CALLBACK_SIGNING_KEY](../../../evidence/sources/authorities.md#src-543ac98fff) — [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py::\_secret](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py) | `values.get(name, '')` |

### Machine authority

- `/external_contract/configuration_environment/246`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e8f876369f0dbb4d7b8f290ced65b7bb555a624696dc1b3438fd3f37586e0731 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-server:environment:STOVE0_TARGET_CALLBACK_SIGNING_KEY",
  "input_shape": "environment-string",
  "name": "STOVE0_TARGET_CALLBACK_SIGNING_KEY",
  "owner": "stove0-server"
}
```

</details>
