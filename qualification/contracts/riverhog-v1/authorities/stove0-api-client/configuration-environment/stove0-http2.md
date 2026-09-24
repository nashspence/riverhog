# STOVE0_HTTP2

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-api-client:stove0-http2:2bc282e272 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-b1ba755d84"></a>

| Field | Value |
|---|---|
| <a id="s-c26d089493"></a>`consumers` | `["stove0-api-client"]` |
| <a id="s-49bffd7273"></a>`default_expressions` | `["unset"]` |
| <a id="s-3789b05811"></a>`id` | `"stove0-api-client:environment:STOVE0_HTTP2"` |
| <a id="s-085e8e61c2"></a>`input_shape` | `"environment-string"` |
| <a id="s-593baac063"></a>`name` | `"STOVE0_HTTP2"` |
| <a id="s-4486e5a845"></a>`owner` | `"stove0-api-client"` |

## Governing policies

- <a id="pa-d02cd184d3"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-api-client:STOVE0_HTTP2](../../../evidence/sources/authorities.md#src-29a943f529) — [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/client.py::\_boolean\_env](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/client.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-api-client` | [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/client.py](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/client.py) | `os.getenv(name)` |

### Machine authority

- `/external_contract/configuration_environment/220`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dbecdef2c4cfee74ba9f3643baeabafd24d4e036694a597ff8f3b2685e97b21f -->

```json
{
  "consumers": [
    "stove0-api-client"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "stove0-api-client:environment:STOVE0_HTTP2",
  "input_shape": "environment-string",
  "name": "STOVE0_HTTP2",
  "owner": "stove0-api-client"
}
```

</details>
