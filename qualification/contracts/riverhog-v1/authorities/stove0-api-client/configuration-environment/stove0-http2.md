# STOVE0_HTTP2

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-api-client:stove0-http2:c5a9c8dcc9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-7c4bc7f8a3"></a>

| Field | Value |
|---|---|
| <a id="s-de3bbf83d5"></a>`consumers` | `["stove0-api-client"]` |
| <a id="s-603fa45834"></a>`default_expressions` | `["unset"]` |
| <a id="s-dc7c1a2e94"></a>`id` | `"stove0-api-client:environment:STOVE0_HTTP2"` |
| <a id="s-45affb291c"></a>`input_shape` | `"environment-string"` |
| <a id="s-033e682cb8"></a>`name` | `"STOVE0_HTTP2"` |
| <a id="s-609af60b93"></a>`owner` | `"stove0-api-client"` |

## Governing policies

- <a id="pa-9fadabf034"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

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

- `/external_contract/configuration_environment/116`

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
