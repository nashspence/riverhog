# STOVE0_ALLOW_INSECURE_HTTP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-api-client:stove0-allow-insecure-http:ac9127f745 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-6d8ed99c25"></a>

| Field | Value |
|---|---|
| <a id="s-455d12eaca"></a>`consumers` | `["stove0-api-client"]` |
| <a id="s-1072ddffa8"></a>`default_expressions` | `["unset"]` |
| <a id="s-70cf382471"></a>`id` | `"stove0-api-client:environment:STOVE0_ALLOW_INSECURE_HTTP"` |
| <a id="s-1b2f5581d2"></a>`input_shape` | `"environment-string"` |
| <a id="s-1b616d0548"></a>`name` | `"STOVE0_ALLOW_INSECURE_HTTP"` |
| <a id="s-8d577f9560"></a>`owner` | `"stove0-api-client"` |

## Governing policies

- <a id="pa-e1e7b1e11d"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-api-client:STOVE0_ALLOW_INSECURE_HTTP](../../../evidence/sources/authorities.md#src-a9f40fdb75) — [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/client.py::\_boolean\_env](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/client.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-api-client` | [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/client.py](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/client.py) | `os.getenv(name)` |

### Machine authority

- `/external_contract/configuration_environment/114`

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
