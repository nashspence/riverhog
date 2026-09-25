# RIVERHOG_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-base-url:028dbb0590 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-551254e2cb"></a>

| Field | Value |
|---|---|
| <a id="s-27a1ad1a86"></a>`consumers` | `["riverhog-client"]` |
| <a id="s-dd3a6cbf0c"></a>`default_expressions` | `["unset"]` |
| <a id="s-3a0a03e6eb"></a>`id` | `"riverhog-client:environment:RIVERHOG_BASE_URL"` |
| <a id="s-21fd72eb86"></a>`input_shape` | `"environment-string"` |
| <a id="s-fd2ae4a599"></a>`name` | `"RIVERHOG_BASE_URL"` |
| <a id="s-50b4a1ec36"></a>`owner` | `"riverhog-client"` |

## Governing policies

- <a id="pa-373f0cb843"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-client:RIVERHOG_BASE_URL](../../../evidence/sources/authorities.md#src-475671fe74) — [packages/riverhog-client/src/riverhog\_client/client.py::\_HttpApiClient.\_\_init\_\_](../../../../../../packages/riverhog-client/src/riverhog_client/client.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-client` | [packages/riverhog-client/src/riverhog\_client/client.py](../../../../../../packages/riverhog-client/src/riverhog_client/client.py) | `os.getenv('RIVERHOG_BASE_URL')` |

### Machine authority

- `/external_contract/configuration_environment/99`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 02c74db099381f19d65370253d80f4ae683173d74e5dcff0a7c9a80bc9cf51b9 -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-client:environment:RIVERHOG_BASE_URL",
  "input_shape": "environment-string",
  "name": "RIVERHOG_BASE_URL",
  "owner": "riverhog-client"
}
```

</details>
