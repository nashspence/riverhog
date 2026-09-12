# RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-cache-new-archive-lease:8c6f623196 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-5c633c982d"></a>
| Field | Shape |
|---|---|
| <a id="s-cd6ec93f3f"></a>`classification` | "runtime" |
| <a id="s-093b7de81f"></a>`consumers` | ["riverhog-server"] |
| <a id="s-330311feff"></a>`disposition` | "contractual" |
| <a id="s-e6605379f6"></a>`id` | "riverhog-server:environment:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE" |
| <a id="s-340ef8e53f"></a>`name` | "RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE" |
| <a id="s-51770eadf0"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE](#s-5c633c982d) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-bbf5bf215e"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-b89e48e852"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE](../../../evidence/sources.md#src-22c1f9d04c) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE', '72h')` |

### Machine authority

- `/external_contract/configuration_environment/65`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b0c8a8f22d5f346d4b3b4ed17cd6037a2459200d597bba25236b63af1d155659 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE",
  "name": "RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE",
  "owner": "riverhog-server"
}
```
