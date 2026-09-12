# RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-prepare-concurrency:997eab1b80 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-6710649fb3"></a>
| Field | Shape |
|---|---|
| <a id="s-9a9e87631d"></a>`classification` | "runtime" |
| <a id="s-fe5e426476"></a>`consumers` | ["riverhog-server"] |
| <a id="s-df985c833e"></a>`disposition` | "contractual" |
| <a id="s-6bfdc5327e"></a>`id` | "riverhog-server:environment:RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY" |
| <a id="s-3a8258723c"></a>`name` | "RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY" |
| <a id="s-373b12e6ec"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY](#s-6710649fb3) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-a020d85e18"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-19db3dd827"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY](../../../evidence/sources.md#src-36a45a4230) — `riverhog/src/riverhog_core/throughput.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/throughput.py` | `_env_int(values, 'RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY', DEFAULT_UPLOAD_PREPARE_CONCURRENCY)` |

### Machine authority

- `/external_contract/configuration_environment/35`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2eb57ee08741e46d680d95d49b68996296628b68d4b86ce88fd1264307c29d1a -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY",
  "name": "RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY",
  "owner": "riverhog-server"
}
```
