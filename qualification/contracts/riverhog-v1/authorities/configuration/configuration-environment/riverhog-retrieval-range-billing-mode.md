# RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-retrieval-range-billing-mode:a00c4fd4f8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-63134bf139"></a>
| Field | Shape |
|---|---|
| <a id="s-7a2481d8f8"></a>`consumers` | ["riverhog-server"] |
| <a id="s-822be7b829"></a>`name` | "RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE" |

## Governing policies

- <a id="pa-b62340c0e0"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE](../../../evidence/sources.md#src-dcbfe988b4) — `configuration-environment:RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/69`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 929878da90a1dc99ea291d3b2f412a65231bc56fc555d570dfa2f7d9575a4976 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE"
}
```
