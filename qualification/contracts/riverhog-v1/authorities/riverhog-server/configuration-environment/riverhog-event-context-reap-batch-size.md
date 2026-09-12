# RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-event-context-reap-batch-size:4caad53032 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-1cd6fe0a97"></a>
| Field | Shape |
|---|---|
| <a id="s-71e578a1e6"></a>`classification` | "runtime" |
| <a id="s-923821698e"></a>`consumers` | ["riverhog-server"] |
| <a id="s-f764dbb689"></a>`disposition` | "contractual" |
| <a id="s-a7a2756f2e"></a>`id` | "riverhog-server:environment:RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE" |
| <a id="s-8f98162f43"></a>`name` | "RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE" |
| <a id="s-5f637bee10"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE](#s-1cd6fe0a97) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-e53911fe0b"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-ae29b20c0a"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE](../../../evidence/sources.md#src-fef401b6a9) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `_parse_int(os.getenv('RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE', '100'), name='RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE', minimum=1)` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE', '100')` |

### Machine authority

- `/external_contract/configuration_environment/53`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e5815ee70a8f078596876a483418c60ac6dae36b2fbe7a1bb44cf5e8eebd2020 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE",
  "name": "RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE",
  "owner": "riverhog-server"
}
```
