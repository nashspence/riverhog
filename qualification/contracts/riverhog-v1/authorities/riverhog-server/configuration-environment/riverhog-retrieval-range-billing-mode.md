# RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-range-billing-mode:4f199d9117 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](families/settings/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-24fc48da99"></a>
| Field | Shape |
|---|---|
| <a id="s-fa86d7a910"></a>`consumers` | ["riverhog-server"] |
| <a id="s-1b714bd214"></a>`default_expressions` | ["unset"] |
| <a id="s-0452f51736"></a>`id` | "riverhog-server:environment:RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE" |
| <a id="s-cb1d246623"></a>`input_shape` | "environment-string" |
| <a id="s-071aa15ddd"></a>`name` | "RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE" |
| <a id="s-40f3826282"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-031af2368f"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE](../../../evidence/sources.md#src-d46028cd7f) — `riverhog/src/riverhog_core/pack_retrieval.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/pack_retrieval.py` | `values.get(global_name)` |

### Machine authority

- `/external_contract/configuration_environment/80`

### Exact owned JSON

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
