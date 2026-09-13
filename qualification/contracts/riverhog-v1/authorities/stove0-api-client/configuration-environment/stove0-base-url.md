# STOVE0_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-api-client:stove0-base-url:beaf204566 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-78853bee33"></a>
| Field | Shape |
|---|---|
| <a id="s-7e93422191"></a>`consumers` | ["stove0-api-client"] |
| <a id="s-2fbfea52d4"></a>`default_expressions` | ["unset"] |
| <a id="s-c3ae61da87"></a>`id` | "stove0-api-client:environment:STOVE0_BASE_URL" |
| <a id="s-83d4c87af5"></a>`input_shape` | "environment-string" |
| <a id="s-5142b62fff"></a>`name` | "STOVE0_BASE_URL" |
| <a id="s-38a4ecd2f9"></a>`owner` | "stove0-api-client" |

## Governing policies

- <a id="pa-5b6692986a"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-api-client:STOVE0_BASE_URL](../../../evidence/sources.md#src-34ec96f5f9) — `reference/stove0/packages/api-client/src/stove0_api_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-api-client` | `reference/stove0/packages/api-client/src/stove0_api_client/client.py` | `os.getenv('STOVE0_BASE_URL')` |

### Machine authority

- `/external_contract/configuration_environment/144`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e6e28a832aaf4d7f7dc2677719f8bd2587ab268171c56c27081844a2bf68172a -->

```json
{
  "consumers": [
    "stove0-api-client"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "stove0-api-client:environment:STOVE0_BASE_URL",
  "input_shape": "environment-string",
  "name": "STOVE0_BASE_URL",
  "owner": "stove0-api-client"
}
```
