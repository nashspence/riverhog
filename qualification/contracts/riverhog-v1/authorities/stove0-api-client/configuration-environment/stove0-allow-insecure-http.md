# STOVE0_ALLOW_INSECURE_HTTP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-api-client:stove0-allow-insecure-http:03dc6fb209 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-d0c45c1a2b"></a>

| Field | Value |
|---|---|
| <a id="s-47cc2d8760"></a>`consumers` | `["stove0-api-client"]` |
| <a id="s-d2d1e00bbb"></a>`default_expressions` | `["unset"]` |
| <a id="s-820d19aacb"></a>`id` | `"stove0-api-client:environment:STOVE0_ALLOW_INSECURE_HTTP"` |
| <a id="s-2953890d77"></a>`input_shape` | `"environment-string"` |
| <a id="s-a1035d4640"></a>`name` | `"STOVE0_ALLOW_INSECURE_HTTP"` |
| <a id="s-5796450659"></a>`owner` | `"stove0-api-client"` |

## Governing policies

- <a id="pa-7777078974"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-api-client:STOVE0_ALLOW_INSECURE_HTTP](../../../evidence/sources.md#src-a9f40fdb75) — [reference/stove0/packages/api-client/src/stove0\_api\_client/client.py::\_boolean\_env](../../../../../../reference/stove0/packages/api-client/src/stove0_api_client/client.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-api-client` | [reference/stove0/packages/api-client/src/stove0\_api\_client/client.py](../../../../../../reference/stove0/packages/api-client/src/stove0_api_client/client.py) | `os.getenv(name)` |

### Machine authority

- `/external_contract/configuration_environment/143`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e8f456c42f63aeac466e31d910135d2cf9a6fa4772223df7134a5fd90c2ca40b -->

```json
{
  "consumers": [
    "stove0-api-client"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "stove0-api-client:environment:STOVE0_ALLOW_INSECURE_HTTP",
  "input_shape": "environment-string",
  "name": "STOVE0_ALLOW_INSECURE_HTTP",
  "owner": "stove0-api-client"
}
```

</details>
