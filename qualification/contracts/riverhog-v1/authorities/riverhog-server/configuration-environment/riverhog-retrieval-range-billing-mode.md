# RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-range-billing-mode:cb52bc0059 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-99c8eb1c7d"></a>

| Field | Value |
|---|---|
| <a id="s-a52c09abae"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-a10389ce2d"></a>`default_expressions` | `["unset"]` |
| <a id="s-62331dcb68"></a>`id` | `"riverhog-server:environment:RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE"` |
| <a id="s-662e6a0e21"></a>`input_shape` | `"environment-string"` |
| <a id="s-9e12e97dee"></a>`name` | `"RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE"` |
| <a id="s-abdde598fa"></a>`owner` | `"riverhog-server"` |

## Governing policies

- <a id="pa-2456f93ea7"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE](../../../evidence/sources/authorities.md#src-d46028cd7f) — [riverhog/src/riverhog\_core/pack\_retrieval.py::\_scoped\_env\_value](../../../../../../riverhog/src/riverhog_core/pack_retrieval.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/pack\_retrieval.py](../../../../../../riverhog/src/riverhog_core/pack_retrieval.py) | `values.get(global_name)` |

### Machine authority

- `/external_contract/configuration_environment/214`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8303cf5bcf67812f54c16c70e0ac1353418a46d8f00084dfbbb23e1de87b209d -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE",
  "owner": "riverhog-server"
}
```

</details>
