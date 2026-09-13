# STOVE0_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-api-client:stove0-token:42bf707593 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-e262a7b7fb"></a>
| Field | Shape |
|---|---|
| <a id="s-c6ad71c6b4"></a>`consumers` | ["stove0-api-client"] |
| <a id="s-f57ba9da34"></a>`default_expressions` | ["unset"] |
| <a id="s-5617dd1d1b"></a>`id` | "stove0-api-client:environment:STOVE0_TOKEN" |
| <a id="s-6cff6cc3f4"></a>`input_shape` | "environment-string" |
| <a id="s-7eccff2ac5"></a>`name` | "STOVE0_TOKEN" |
| <a id="s-32a8ee0908"></a>`owner` | "stove0-api-client" |

## Governing policies

- <a id="pa-730087eddc"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-api-client:STOVE0_TOKEN](../../../evidence/sources.md#src-c052f8f5ca) — `reference/stove0/packages/api-client/src/stove0_api_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-api-client` | `reference/stove0/packages/api-client/src/stove0_api_client/client.py` | `os.getenv('STOVE0_TOKEN')` |

### Machine authority

- `/external_contract/configuration_environment/147`

### Exact owned JSON

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
