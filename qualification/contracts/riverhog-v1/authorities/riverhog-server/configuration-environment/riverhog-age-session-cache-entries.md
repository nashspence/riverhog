# RIVERHOG_AGE_SESSION_CACHE_ENTRIES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-age-session-cache-entries:061c38dc37 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-2d908ff8df"></a>
| Field | Shape |
|---|---|
| <a id="s-42b74a58c7"></a>`classification` | "runtime" |
| <a id="s-c799298a70"></a>`consumers` | ["riverhog-server"] |
| <a id="s-8064544054"></a>`disposition` | "contractual" |
| <a id="s-69dffd9a14"></a>`id` | "riverhog-server:environment:RIVERHOG_AGE_SESSION_CACHE_ENTRIES" |
| <a id="s-c530fa2f45"></a>`name` | "RIVERHOG_AGE_SESSION_CACHE_ENTRIES" |
| <a id="s-c6f518f261"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_AGE_SESSION_CACHE_ENTRIES"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_AGE_SESSION_CACHE_ENTRIES](#s-2d908ff8df) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-4f2d48226b"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-204ca18fe2"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_AGE_SESSION_CACHE_ENTRIES](../../../evidence/sources.md#src-d1018a4c53) — `riverhog/src/riverhog_core/throughput.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/throughput.py` | `_env_int(values, 'RIVERHOG_AGE_SESSION_CACHE_ENTRIES', DEFAULT_AGE_SESSION_CACHE_ENTRIES)` |

### Machine authority

- `/external_contract/configuration_environment/30`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c7d2abceb52cbbf44a30d722028833382edaebd3b85167e64cba56ac04b6e1b6 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_AGE_SESSION_CACHE_ENTRIES",
  "name": "RIVERHOG_AGE_SESSION_CACHE_ENTRIES",
  "owner": "riverhog-server"
}
```
