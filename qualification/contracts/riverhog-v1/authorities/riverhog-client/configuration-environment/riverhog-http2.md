# RIVERHOG_HTTP2

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-http2:da2b968a34 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-549c81fb7a"></a>

| Field | Value |
|---|---|
| <a id="s-da01a86256"></a>`consumers` | `["riverhog-client"]` |
| <a id="s-600627c040"></a>`default_expressions` | `["unset"]` |
| <a id="s-b1b61e3fa9"></a>`id` | `"riverhog-client:environment:RIVERHOG_HTTP2"` |
| <a id="s-59a3425c54"></a>`input_shape` | `"environment-string"` |
| <a id="s-dd46bf31d6"></a>`name` | `"RIVERHOG_HTTP2"` |
| <a id="s-6bf4797208"></a>`owner` | `"riverhog-client"` |

## Governing policies

- <a id="pa-daa6ea1a54"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-client:RIVERHOG_HTTP2](../../../evidence/sources/authorities.md#src-1774dabb11) — [packages/riverhog-client/src/riverhog\_client/client.py::\_bool\_env](../../../../../../packages/riverhog-client/src/riverhog_client/client.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-client` | [packages/riverhog-client/src/riverhog\_client/client.py](../../../../../../packages/riverhog-client/src/riverhog_client/client.py) | `os.getenv(env_name)` |

### Machine authority

- `/external_contract/configuration_environment/104`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: af852d0d95151fb425b476d832bb2ce45ff4163c5730a9a98842d8f0a44ff856 -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-client:environment:RIVERHOG_HTTP2",
  "input_shape": "environment-string",
  "name": "RIVERHOG_HTTP2",
  "owner": "riverhog-client"
}
```

</details>
