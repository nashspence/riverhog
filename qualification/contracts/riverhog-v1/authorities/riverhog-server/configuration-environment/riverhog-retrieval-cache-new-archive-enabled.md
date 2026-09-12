# RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-cache-new-archive-enabled:b6bfcf02fe -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1c368e3bbb"></a>
| Field | Shape |
|---|---|
| <a id="s-da73de28a8"></a>`classification` | "runtime" |
| <a id="s-0e717d7f5c"></a>`consumers` | ["riverhog-server"] |
| <a id="s-b712f80856"></a>`disposition` | "contractual" |
| <a id="s-8acc408344"></a>`id` | "riverhog-server:environment:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED" |
| <a id="s-561f90128c"></a>`name` | "RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED" |
| <a id="s-2750bfabee"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-0ab33d1a5c"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED](../../../evidence/sources.md#src-257c42be6d) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED', 'true')` |

### Machine authority

- `/external_contract/configuration_environment/64`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 20781be8ae012f7a53679d01e7009ae124265e742b84a3ffae564b7be638fa9f -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED",
  "name": "RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED",
  "owner": "riverhog-server"
}
```
