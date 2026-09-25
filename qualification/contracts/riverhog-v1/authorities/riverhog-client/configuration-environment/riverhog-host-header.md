# RIVERHOG_HOST_HEADER

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-host-header:fa645b17e7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-9f9eafd5d5"></a>

| Field | Value |
|---|---|
| <a id="s-8248147251"></a>`consumers` | `["riverhog-client"]` |
| <a id="s-be154e911d"></a>`default_expressions` | `["''"]` |
| <a id="s-8211605975"></a>`id` | `"riverhog-client:environment:RIVERHOG_HOST_HEADER"` |
| <a id="s-c550564cdf"></a>`input_shape` | `"environment-string"` |
| <a id="s-0e24792ee3"></a>`name` | `"RIVERHOG_HOST_HEADER"` |
| <a id="s-48214e7d5d"></a>`owner` | `"riverhog-client"` |

## Governing policies

- <a id="pa-46f8c796e4"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-client:RIVERHOG_HOST_HEADER](../../../evidence/sources/authorities.md#src-9a32401dbf) — [packages/riverhog-client/src/riverhog\_client/client.py::\_HttpApiClient.\_\_init\_\_](../../../../../../packages/riverhog-client/src/riverhog_client/client.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-client` | [packages/riverhog-client/src/riverhog\_client/client.py](../../../../../../packages/riverhog-client/src/riverhog_client/client.py) | `os.getenv('RIVERHOG_HOST_HEADER', '')` |

### Machine authority

- `/external_contract/configuration_environment/103`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3a9eafd78acb18a1522a4c2afb3917e19c0ac682975d1c79f9cdf537b3ec0a3d -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-client:environment:RIVERHOG_HOST_HEADER",
  "input_shape": "environment-string",
  "name": "RIVERHOG_HOST_HEADER",
  "owner": "riverhog-client"
}
```

</details>
