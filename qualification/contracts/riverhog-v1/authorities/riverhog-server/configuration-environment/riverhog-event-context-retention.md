# RIVERHOG_EVENT_CONTEXT_RETENTION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-event-context-retention:c7b133b8c7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-add94be491"></a>
| Field | Shape |
|---|---|
| <a id="s-7818c5ee43"></a>`classification` | "runtime" |
| <a id="s-d2bd3c7959"></a>`consumers` | ["riverhog-server"] |
| <a id="s-6531698e54"></a>`disposition` | "contractual" |
| <a id="s-b3a8cf981b"></a>`id` | "riverhog-server:environment:RIVERHOG_EVENT_CONTEXT_RETENTION" |
| <a id="s-66e2078736"></a>`name` | "RIVERHOG_EVENT_CONTEXT_RETENTION" |
| <a id="s-263bfe3ca9"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_EVENT_CONTEXT_RETENTION"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_EVENT_CONTEXT_RETENTION](#s-add94be491) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-42e7c83611"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-0215070d45"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_EVENT_CONTEXT_RETENTION](../../../evidence/sources.md#src-42756a2b0a) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_EVENT_CONTEXT_RETENTION', '30d')` |

### Machine authority

- `/external_contract/configuration_environment/54`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ae9d56d3f6f7b612b1cbd7ebad4e89a312d41cb261901f295631b0b6f5ee9972 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_EVENT_CONTEXT_RETENTION",
  "name": "RIVERHOG_EVENT_CONTEXT_RETENTION",
  "owner": "riverhog-server"
}
```
