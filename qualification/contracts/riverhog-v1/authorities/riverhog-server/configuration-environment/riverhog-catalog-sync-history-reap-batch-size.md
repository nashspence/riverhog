# RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-catalog-sync-history-reap-batch-size:74691fba77 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-6d67d56ce0"></a>
| Field | Shape |
|---|---|
| <a id="s-9863a1dc4e"></a>`classification` | "runtime" |
| <a id="s-1a3e8ba501"></a>`consumers` | ["riverhog-server"] |
| <a id="s-e70d2d7c5c"></a>`disposition` | "contractual" |
| <a id="s-c1495d444f"></a>`id` | "riverhog-server:environment:RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE" |
| <a id="s-639a4bef7f"></a>`name` | "RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE" |
| <a id="s-a908c852ad"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE](#s-6d67d56ce0) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-6cf83ded3e"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-4e8d4d5fb6"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE](../../../evidence/sources.md#src-e04d565c18) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `_parse_int(os.getenv('RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE', '100'), name='RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE', minimum=1)` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE', '100')` |

### Machine authority

- `/external_contract/configuration_environment/48`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e16095a5b215d26d26b6e0a701f9814827254cdada2ee63343bdae44f140bbc1 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE",
  "name": "RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE",
  "owner": "riverhog-server"
}
```
