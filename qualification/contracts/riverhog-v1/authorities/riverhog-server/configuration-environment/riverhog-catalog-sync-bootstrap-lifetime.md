# RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-catalog-sync-bootstrap-lifetime:24b4698652 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-c5d59cd44b"></a>
| Field | Shape |
|---|---|
| <a id="s-5ce823e9af"></a>`classification` | "runtime" |
| <a id="s-4676feab29"></a>`consumers` | ["riverhog-server"] |
| <a id="s-0f0276ea65"></a>`disposition` | "contractual" |
| <a id="s-8e21124594"></a>`id` | "riverhog-server:environment:RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME" |
| <a id="s-0774de54c0"></a>`name` | "RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME" |
| <a id="s-020c7424d1"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-77c5662fff"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME](../../../evidence/sources.md#src-bb4e9ed43a) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME', '7d')` |

### Machine authority

- `/external_contract/configuration_environment/46`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a3bf97260f04b707881cdf47e143f685f99a00a181310322e52b08bbc69a73c7 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME",
  "name": "RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME",
  "owner": "riverhog-server"
}
```
