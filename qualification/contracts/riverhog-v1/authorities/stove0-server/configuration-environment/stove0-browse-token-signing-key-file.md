# STOVE0_BROWSE_TOKEN_SIGNING_KEY_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-browse-token-signing-key-file:26fa069e53 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-4100c93200"></a>

| Field | Value |
|---|---|
| <a id="s-23f3e07db6"></a>`consumers` | `["stove0-server"]` |
| <a id="s-1e00eaa47b"></a>`default_expressions` | `["''"]` |
| <a id="s-1c7f09fb76"></a>`id` | `"stove0-server:environment:STOVE0_BROWSE_TOKEN_SIGNING_KEY_FILE"` |
| <a id="s-875aa8c93f"></a>`input_shape` | `"environment-string"` |
| <a id="s-c0627a9cb2"></a>`name` | `"STOVE0_BROWSE_TOKEN_SIGNING_KEY_FILE"` |
| <a id="s-ffaf3a09a2"></a>`owner` | `"stove0-server"` |

## Governing policies

- <a id="pa-1d7fc54422"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_BROWSE_TOKEN_SIGNING_KEY_FILE](../../../evidence/sources/authorities.md#src-4c41ceb811) — [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py::\_secret](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py) | `values.get(f'{name}_FILE', '')` |

### Machine authority

- `/external_contract/configuration_environment/232`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e85dfb815ddec1f3f96b2f52a6e6fea461a28e272f7966aecd4f31bb722ff391 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-server:environment:STOVE0_BROWSE_TOKEN_SIGNING_KEY_FILE",
  "input_shape": "environment-string",
  "name": "STOVE0_BROWSE_TOKEN_SIGNING_KEY_FILE",
  "owner": "stove0-server"
}
```

</details>
