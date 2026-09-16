# RIVERHOG_ALLOW_INSECURE_HTTP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-allow-insecure-http:1ccb08eb7b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-b6148dfd81"></a>

| Field | Value |
|---|---|
| <a id="s-b49faf4a7f"></a>`consumers` | `["riverhog-client"]` |
| <a id="s-e4135fc5a3"></a>`default_expressions` | `["unset"]` |
| <a id="s-23f3a2f19a"></a>`id` | `"riverhog-client:environment:RIVERHOG_ALLOW_INSECURE_HTTP"` |
| <a id="s-41c7764688"></a>`input_shape` | `"environment-string"` |
| <a id="s-a2dcbb91e6"></a>`name` | `"RIVERHOG_ALLOW_INSECURE_HTTP"` |
| <a id="s-392cf8391f"></a>`owner` | `"riverhog-client"` |

## Governing policies

- <a id="pa-a2ec68e706"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-client:RIVERHOG_ALLOW_INSECURE_HTTP](../../../evidence/sources.md#src-6501ce08a3) — `packages/riverhog-client/src/riverhog_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-client` | `packages/riverhog-client/src/riverhog_client/client.py` | `os.getenv(env_name)` |

### Machine authority

- `/external_contract/configuration_environment/12`

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
