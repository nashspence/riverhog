# STOVE0_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-api-client:stove0-base-url:f24cc861b3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-dfda7a7a4a"></a>

| Field | Value |
|---|---|
| <a id="s-47f3bdaf5e"></a>`consumers` | `["stove0-api-client"]` |
| <a id="s-e3b07c3ef7"></a>`default_expressions` | `["unset"]` |
| <a id="s-81786082bc"></a>`id` | `"stove0-api-client:environment:STOVE0_BASE_URL"` |
| <a id="s-a71ba1fcc4"></a>`input_shape` | `"environment-string"` |
| <a id="s-9d1fbfc618"></a>`name` | `"STOVE0_BASE_URL"` |
| <a id="s-9d55402f49"></a>`owner` | `"stove0-api-client"` |

## Governing policies

- <a id="pa-6446b17ce2"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-api-client:STOVE0_BASE_URL](../../../evidence/sources/authorities.md#src-34ec96f5f9) — [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/client.py::Stove0ApiClient.\_\_init\_\_](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/client.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-api-client` | [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/client.py](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/client.py) | `os.getenv('STOVE0_BASE_URL')` |

### Machine authority

- `/external_contract/configuration_environment/219`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
