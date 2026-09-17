# STOVE0_API_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-api-token-file:5ec13dd4f6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-1e11f442b1"></a>

| Field | Value |
|---|---|
| <a id="s-1da413efaf"></a>`consumers` | `["stove0-server"]` |
| <a id="s-0d19b67e16"></a>`default_expressions` | `["''"]` |
| <a id="s-ffe3cd761b"></a>`id` | `"stove0-server:environment:STOVE0_API_TOKEN_FILE"` |
| <a id="s-a9dc398834"></a>`input_shape` | `"environment-string"` |
| <a id="s-6c7bb367ae"></a>`name` | `"STOVE0_API_TOKEN_FILE"` |
| <a id="s-7f6a641ee0"></a>`owner` | `"stove0-server"` |

## Governing policies

- <a id="pa-b62bb7edcd"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_API_TOKEN_FILE](../../../evidence/sources/authorities.md#src-5b16a09dac) — [reference/stove0/application/server/src/stove0\_core/runtime\_config.py::\_secret](../../../../../../reference/stove0/application/server/src/stove0_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | [reference/stove0/application/server/src/stove0\_core/runtime\_config.py](../../../../../../reference/stove0/application/server/src/stove0_core/runtime_config.py) | `values.get(f'{name}_FILE', '')` |

### Machine authority

- `/external_contract/configuration_environment/230`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b4d762210ac23d0c57b318bb741259b7ac56adbe5beb1e8f4d46d96012e9321b -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-server:environment:STOVE0_API_TOKEN_FILE",
  "input_shape": "environment-string",
  "name": "STOVE0_API_TOKEN_FILE",
  "owner": "stove0-server"
}
```

</details>
