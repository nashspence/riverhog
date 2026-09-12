# RIVERHOG_RETRIEVAL_MAX_LEASE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-max-lease:1aa0aa0886 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-abc6c0ac51"></a>
| Field | Shape |
|---|---|
| <a id="s-d335f9430f"></a>`classification` | "runtime" |
| <a id="s-1ed351fe80"></a>`consumers` | ["riverhog-server"] |
| <a id="s-746e89730f"></a>`disposition` | "contractual" |
| <a id="s-d14f4cb1fe"></a>`id` | "riverhog-server:environment:RIVERHOG_RETRIEVAL_MAX_LEASE" |
| <a id="s-b3f8b0388d"></a>`name` | "RIVERHOG_RETRIEVAL_MAX_LEASE" |
| <a id="s-92764138c3"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_MAX_LEASE"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_MAX_LEASE](#s-abc6c0ac51) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-33c8269f33"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-ddc9f86864"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_MAX_LEASE](../../../evidence/sources.md#src-c44aeb9ce3) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_RETRIEVAL_MAX_LEASE', '7d')` |

### Machine authority

- `/external_contract/configuration_environment/72`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bddbfde34a0828910836c4cd67fea558d43863b2a8a06f52d7312fa5f4c53267 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_MAX_LEASE",
  "name": "RIVERHOG_RETRIEVAL_MAX_LEASE",
  "owner": "riverhog-server"
}
```
