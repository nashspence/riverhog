# RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-max-range-bytes:1444621908 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-a4749ac7f3"></a>
| Field | Shape |
|---|---|
| <a id="s-3ab95cac9e"></a>`classification` | "runtime" |
| <a id="s-a43b0da62e"></a>`consumers` | ["riverhog-server"] |
| <a id="s-a5fedf5e31"></a>`disposition` | "contractual" |
| <a id="s-bff0e51f69"></a>`id` | "riverhog-server:environment:RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES" |
| <a id="s-45655e8c1b"></a>`name` | "RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES" |
| <a id="s-6c4637bf18"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES](#s-a4749ac7f3) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-46c68f6c53"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-2b8ef9a61b"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES](../../../evidence/sources.md#src-1e662f4cb2) — `riverhog/src/riverhog_core/pack_retrieval.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/pack_retrieval.py` | `_scoped_env_bytes(values, 'RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES', DEFAULT_MAX_RANGE_REQUEST_BYTES, store_name=store_name)` |

### Machine authority

- `/external_contract/configuration_environment/73`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7b2768504c201d678d33d8fe904ea01bbdc65b2f1287dc248aae3c1750858b2e -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES",
  "name": "RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES",
  "owner": "riverhog-server"
}
```
