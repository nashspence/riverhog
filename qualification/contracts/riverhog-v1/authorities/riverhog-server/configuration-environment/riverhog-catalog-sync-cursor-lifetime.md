# RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-catalog-sync-cursor-lifetime:b30dcb1ebc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-5581cd63e1"></a>
| Field | Shape |
|---|---|
| <a id="s-f176e21916"></a>`classification` | "runtime" |
| <a id="s-77d05a28fd"></a>`consumers` | ["riverhog-server"] |
| <a id="s-91d5284cd4"></a>`disposition` | "contractual" |
| <a id="s-f39456d089"></a>`id` | "riverhog-server:environment:RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME" |
| <a id="s-f0549843d7"></a>`name` | "RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME" |
| <a id="s-4055a76d48"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-91a147fe98"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME](../../../evidence/sources.md#src-afa71403d4) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME', '24h')` |

### Machine authority

- `/external_contract/configuration_environment/47`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 853feadfc8548fc0999bc59a35f802c9576547eaaf8b3734028eaed51c7701b5 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME",
  "name": "RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME",
  "owner": "riverhog-server"
}
```
