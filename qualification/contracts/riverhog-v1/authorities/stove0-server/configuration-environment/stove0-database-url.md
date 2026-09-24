# STOVE0_DATABASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-database-url:5764dea24f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-2832b8e991"></a>

| Field | Value |
|---|---|
| <a id="s-8fb4f3e4c3"></a>`consumers` | `["stove0-server"]` |
| <a id="s-f8450a61d2"></a>`default_expressions` | `["''"]` |
| <a id="s-77881b1fba"></a>`id` | `"stove0-server:environment:STOVE0_DATABASE_URL"` |
| <a id="s-fc0a93c82e"></a>`input_shape` | `"environment-string"` |
| <a id="s-2656b48bb7"></a>`name` | `"STOVE0_DATABASE_URL"` |
| <a id="s-25093ba074"></a>`owner` | `"stove0-server"` |

## Governing policies

- <a id="pa-c05dfa6920"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_DATABASE_URL](../../../evidence/sources/authorities.md#src-d05255fc63) — [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py::\_secret](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py) | `values.get(name, '')` |

### Machine authority

- `/external_contract/configuration_environment/235`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f53eb30bcd250d2ad1801d7b47d0fa69319347ed41fc6f500b43c47e10783bcd -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-server:environment:STOVE0_DATABASE_URL",
  "input_shape": "environment-string",
  "name": "STOVE0_DATABASE_URL",
  "owner": "stove0-server"
}
```

</details>
