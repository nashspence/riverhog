# STOVE0_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-api-client:stove0-token:5fbf398cf4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-15035a28e0"></a>

| Field | Value |
|---|---|
| <a id="s-950ec387ff"></a>`consumers` | `["stove0-api-client"]` |
| <a id="s-ad12dc509a"></a>`default_expressions` | `["unset"]` |
| <a id="s-85a7db9bee"></a>`id` | `"stove0-api-client:environment:STOVE0_TOKEN"` |
| <a id="s-48f3fe11a2"></a>`input_shape` | `"environment-string"` |
| <a id="s-20ead7c234"></a>`name` | `"STOVE0_TOKEN"` |
| <a id="s-b5a7f86294"></a>`owner` | `"stove0-api-client"` |

## Governing policies

- <a id="pa-69191ea63d"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-api-client:STOVE0_TOKEN](../../../evidence/sources/authorities.md#src-c052f8f5ca) — [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/client.py::Stove0ApiClient.\_\_init\_\_](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/client.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-api-client` | [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/client.py](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/client.py) | `os.getenv('STOVE0_TOKEN')` |

### Machine authority

- `/external_contract/configuration_environment/222`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 22e022bab5ca0eb5fc094156d0a0a0b5897368b04dc819e849709106fbffc5e5 -->

```json
{
  "consumers": [
    "stove0-api-client"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "stove0-api-client:environment:STOVE0_TOKEN",
  "input_shape": "environment-string",
  "name": "STOVE0_TOKEN",
  "owner": "stove0-api-client"
}
```

</details>
