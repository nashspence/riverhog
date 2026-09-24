# STOVE0_BROWSE_TOKEN_SIGNING_KEY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-browse-token-signing-key:d69b8eea10 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-f1fbb5a5d4"></a>

| Field | Value |
|---|---|
| <a id="s-3783a83c9c"></a>`consumers` | `["stove0-server"]` |
| <a id="s-55628ac85d"></a>`default_expressions` | `["''"]` |
| <a id="s-bf201c7363"></a>`id` | `"stove0-server:environment:STOVE0_BROWSE_TOKEN_SIGNING_KEY"` |
| <a id="s-66ede93aca"></a>`input_shape` | `"environment-string"` |
| <a id="s-54e94dbbfa"></a>`name` | `"STOVE0_BROWSE_TOKEN_SIGNING_KEY"` |
| <a id="s-c25d212d2e"></a>`owner` | `"stove0-server"` |

## Governing policies

- <a id="pa-8d9d8f74da"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_BROWSE_TOKEN_SIGNING_KEY](../../../evidence/sources/authorities.md#src-f552366814) — [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py::\_secret](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py) | `values.get(name, '')` |

### Machine authority

- `/external_contract/configuration_environment/231`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 89c3493c390ee480a603e582cf34f29de33602a186b4d29808e60f7ae416689a -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-server:environment:STOVE0_BROWSE_TOKEN_SIGNING_KEY",
  "input_shape": "environment-string",
  "name": "STOVE0_BROWSE_TOKEN_SIGNING_KEY",
  "owner": "stove0-server"
}
```

</details>
