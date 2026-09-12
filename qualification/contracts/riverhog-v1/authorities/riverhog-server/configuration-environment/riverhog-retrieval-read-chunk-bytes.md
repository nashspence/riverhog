# RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-read-chunk-bytes:30e58b20f8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-c741de7c16"></a>
| Field | Shape |
|---|---|
| <a id="s-1a9a7125f4"></a>`classification` | "runtime" |
| <a id="s-3954c61aa8"></a>`consumers` | ["riverhog-server"] |
| <a id="s-06197fcf8d"></a>`disposition` | "contractual" |
| <a id="s-e3fea57c53"></a>`id` | "riverhog-server:environment:RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES" |
| <a id="s-ee62ea9044"></a>`name` | "RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES" |
| <a id="s-175cffa6ac"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES](#s-c741de7c16) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-2d110a3c51"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-1df8156067"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES](../../../evidence/sources.md#src-236f08fcd9) — `riverhog/src/riverhog_core/throughput.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/throughput.py` | `_env_bytes(values, 'RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES', DEFAULT_RETRIEVAL_READ_CHUNK_BYTES)` |

### Machine authority

- `/external_contract/configuration_environment/77`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 29d268d23cd7f72c829bf4dddf0a712149daceffdbc03210026b7b96abcc7d0b -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES",
  "name": "RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES",
  "owner": "riverhog-server"
}
```
