# RIVERHOG_RETRIEVAL_RANGE_MERGE_GAP_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-range-merge-gap-bytes:1a80f5d80a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-0e8ce1fd7c"></a>
| Field | Shape |
|---|---|
| <a id="s-d8d91dcccb"></a>`classification` | "runtime" |
| <a id="s-8e543c475e"></a>`consumers` | ["riverhog-server"] |
| <a id="s-45c37da8a4"></a>`disposition` | "contractual" |
| <a id="s-c7d082cc1f"></a>`id` | "riverhog-server:environment:RIVERHOG_RETRIEVAL_RANGE_MERGE_GAP_BYTES" |
| <a id="s-38ce56b288"></a>`name` | "RIVERHOG_RETRIEVAL_RANGE_MERGE_GAP_BYTES" |
| <a id="s-efc59f7a1f"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_RANGE_MERGE_GAP_BYTES"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_RANGE_MERGE_GAP_BYTES](#s-0e8ce1fd7c) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-988ef35884"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-7a992b1f5e"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_RANGE_MERGE_GAP_BYTES](../../../evidence/sources.md#src-972a8351e5) — `riverhog/src/riverhog_core/pack_retrieval.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/pack_retrieval.py` | `_scoped_env_bytes(values, 'RIVERHOG_RETRIEVAL_RANGE_MERGE_GAP_BYTES', 0, store_name=store_name)` |

### Machine authority

- `/external_contract/configuration_environment/76`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bd40797f2c7510ba9756ae906b8df58298077114da82357f71fd1124aa830ae4 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_RANGE_MERGE_GAP_BYTES",
  "name": "RIVERHOG_RETRIEVAL_RANGE_MERGE_GAP_BYTES",
  "owner": "riverhog-server"
}
```
