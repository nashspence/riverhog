# RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-catalog-sync-history-retention:cb1ffb3cfb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-be3c1ab328"></a>
| Field | Shape |
|---|---|
| <a id="s-2bccaf5535"></a>`classification` | "runtime" |
| <a id="s-53a8b95257"></a>`consumers` | ["riverhog-server"] |
| <a id="s-6a79a68ae5"></a>`disposition` | "contractual" |
| <a id="s-0695702476"></a>`id` | "riverhog-server:environment:RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION" |
| <a id="s-e79d9952b9"></a>`name` | "RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION" |
| <a id="s-5894963f53"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION](#s-be3c1ab328) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-97cf1909da"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-89394e9acc"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION](../../../evidence/sources.md#src-735831a8d4) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION', '30d')` |

### Machine authority

- `/external_contract/configuration_environment/49`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7d76fc5de699e15e1b0754c1bb181d2a59cec409b78beb9cbbe172bd926a5fca -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION",
  "name": "RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION",
  "owner": "riverhog-server"
}
```
