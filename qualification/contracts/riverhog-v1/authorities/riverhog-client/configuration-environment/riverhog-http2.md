# RIVERHOG_HTTP2

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-http2:6c2512da57 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-ef80127a53"></a>

| Field | Value |
|---|---|
| <a id="s-bf7f86f2e0"></a>`consumers` | `["riverhog-client"]` |
| <a id="s-113fcf79e0"></a>`default_expressions` | `["unset"]` |
| <a id="s-e8c3dd7148"></a>`id` | `"riverhog-client:environment:RIVERHOG_HTTP2"` |
| <a id="s-3a65abf7e1"></a>`input_shape` | `"environment-string"` |
| <a id="s-53140a7a23"></a>`name` | `"RIVERHOG_HTTP2"` |
| <a id="s-69c825bdc0"></a>`owner` | `"riverhog-client"` |

## Governing policies

- <a id="pa-b0de139d32"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-client:RIVERHOG_HTTP2](../../../evidence/sources.md#src-1774dabb11) — `packages/riverhog-client/src/riverhog_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-client` | `packages/riverhog-client/src/riverhog_client/client.py` | `os.getenv(env_name)` |

### Machine authority

- `/external_contract/configuration_environment/18`

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
