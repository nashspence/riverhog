# RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-ingress-source-read-chunk-bytes:ca00cdd5ae -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-79f8ec8b66"></a>
| Field | Shape |
|---|---|
| <a id="s-ff4ac2a6d7"></a>`classification` | "runtime" |
| <a id="s-3b66af5af9"></a>`consumers` | ["riverhog-server"] |
| <a id="s-aa68141c3c"></a>`disposition` | "contractual" |
| <a id="s-523002be62"></a>`id` | "riverhog-server:environment:RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES" |
| <a id="s-af9e9db7e5"></a>`name` | "RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES" |
| <a id="s-228a0a0fe8"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES](#s-79f8ec8b66) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-501c901368"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-2f324217ee"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES](../../../evidence/sources.md#src-bf9ecd1b0a) — `riverhog/src/riverhog_core/throughput.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/throughput.py` | `_env_bytes(values, 'RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES', DEFAULT_SOURCE_READ_CHUNK_BYTES)` |

### Machine authority

- `/external_contract/configuration_environment/57`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 861da022cfbb4b8ab947e778a18ea4ba176a26a835f8f6ccb7150f9c2557d628 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES",
  "name": "RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES",
  "owner": "riverhog-server"
}
```
