# RIVERHOG_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-token:e753345b9a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-29be11acec"></a>

| Field | Value |
|---|---|
| <a id="s-9e151f5d6f"></a>`consumers` | `["riverhog-client"]` |
| <a id="s-bf93b03aae"></a>`default_expressions` | `["unset"]` |
| <a id="s-453e030848"></a>`id` | `"riverhog-client:environment:RIVERHOG_TOKEN"` |
| <a id="s-0676e4ac00"></a>`input_shape` | `"environment-string"` |
| <a id="s-7b3cc3529a"></a>`name` | `"RIVERHOG_TOKEN"` |
| <a id="s-254f42a6bc"></a>`owner` | `"riverhog-client"` |

## Governing policies

- <a id="pa-7cc6ae640c"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-client:RIVERHOG_TOKEN](../../../evidence/sources/authorities.md#src-cc55a54933) — [packages/riverhog-client/src/riverhog\_client/client.py::\_HttpApiClient.\_\_init\_\_](../../../../../../packages/riverhog-client/src/riverhog_client/client.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-client` | [packages/riverhog-client/src/riverhog\_client/client.py](../../../../../../packages/riverhog-client/src/riverhog_client/client.py) | `os.getenv(token_env)` |

### Machine authority

- `/external_contract/configuration_environment/162`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 37918e0881f69fd474d697a8787a0a3c9ddb52287090a95b6257fab83176f67f -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-client:environment:RIVERHOG_TOKEN",
  "input_shape": "environment-string",
  "name": "RIVERHOG_TOKEN",
  "owner": "riverhog-client"
}
```

</details>
