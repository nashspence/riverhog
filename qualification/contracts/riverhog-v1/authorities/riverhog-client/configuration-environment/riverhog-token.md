# RIVERHOG_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-token:f05f510122 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-c9bbf6ae4d"></a>
| Field | Shape |
|---|---|
| <a id="s-f8e9201576"></a>`consumers` | ["riverhog-client"] |
| <a id="s-d4eff3f5c1"></a>`default_expressions` | ["unset"] |
| <a id="s-cf2890bc74"></a>`id` | "riverhog-client:environment:RIVERHOG_TOKEN" |
| <a id="s-d5025fbc5e"></a>`input_shape` | "environment-string" |
| <a id="s-3ea82a14dd"></a>`name` | "RIVERHOG_TOKEN" |
| <a id="s-12f813c67b"></a>`owner` | "riverhog-client" |

## Governing policies

- <a id="pa-bcc80691e8"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-client:RIVERHOG_TOKEN](../../../evidence/sources.md#src-cc55a54933) — `packages/riverhog-client/src/riverhog_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-client` | `packages/riverhog-client/src/riverhog_client/client.py` | `os.getenv(token_env)` |

### Machine authority

- `/external_contract/configuration_environment/20`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 37918e0881f69fd474d697a8787a0a3c9ddb52287090a95b6257fab83176f67f -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-client:environment:RIVERHOG_TOKEN",
  "input_shape": "environment-string",
  "name": "RIVERHOG_TOKEN",
  "owner": "riverhog-client"
}
```
