# RIVERHOG_PACK_FILES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-pack-files:36e9705cd3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-749bd6e141"></a>
| Field | Shape |
|---|---|
| <a id="s-cdd8cb4969"></a>`classification` | "runtime" |
| <a id="s-032458916b"></a>`consumers` | ["riverhog-server"] |
| <a id="s-65586fb416"></a>`disposition` | "contractual" |
| <a id="s-6b131cbb88"></a>`id` | "riverhog-server:environment:RIVERHOG_PACK_FILES" |
| <a id="s-94ad8621b7"></a>`name` | "RIVERHOG_PACK_FILES" |
| <a id="s-c4e479ca84"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-d05291dbaf"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_PACK_FILES](../../../evidence/sources.md#src-7d59fe1eef) — `riverhog/src/riverhog_core/collection_plan.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/collection_plan.py` | `_env_int(values, 'RIVERHOG_PACK_FILES', ARCHIVE_PACK_FILES_MAX)` |

### Machine authority

- `/external_contract/configuration_environment/59`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 12d62f0ee0a7a59e1d6eab2a330fcdd67bfce39e33d166a38d6ec612620a8398 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_PACK_FILES",
  "name": "RIVERHOG_PACK_FILES",
  "owner": "riverhog-server"
}
```
