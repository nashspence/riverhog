# RIVERHOG_ALLOW_INSECURE_HTTP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-allow-insecure-http:ce46645025 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-3f89961eb2"></a>

| Field | Value |
|---|---|
| <a id="s-67e906ba4c"></a>`consumers` | `["riverhog-client"]` |
| <a id="s-bff9a69f3a"></a>`default_expressions` | `["unset"]` |
| <a id="s-96bdabf9ee"></a>`id` | `"riverhog-client:environment:RIVERHOG_ALLOW_INSECURE_HTTP"` |
| <a id="s-6d52655fc2"></a>`input_shape` | `"environment-string"` |
| <a id="s-a96edc8bcf"></a>`name` | `"RIVERHOG_ALLOW_INSECURE_HTTP"` |
| <a id="s-749493ebc6"></a>`owner` | `"riverhog-client"` |

## Governing policies

- <a id="pa-d68c2a1c03"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-client:RIVERHOG_ALLOW_INSECURE_HTTP](../../../evidence/sources/authorities.md#src-6501ce08a3) — [packages/riverhog-client/src/riverhog\_client/client.py::\_bool\_env](../../../../../../packages/riverhog-client/src/riverhog_client/client.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-client` | [packages/riverhog-client/src/riverhog\_client/client.py](../../../../../../packages/riverhog-client/src/riverhog_client/client.py) | `os.getenv(env_name)` |

### Machine authority

- `/external_contract/configuration_environment/154`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1fec2434f6a140a54bc65a05045a439cd6bdad01f154b377a2be7bf64ee81806 -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-client:environment:RIVERHOG_ALLOW_INSECURE_HTTP",
  "input_shape": "environment-string",
  "name": "RIVERHOG_ALLOW_INSECURE_HTTP",
  "owner": "riverhog-client"
}
```

</details>
