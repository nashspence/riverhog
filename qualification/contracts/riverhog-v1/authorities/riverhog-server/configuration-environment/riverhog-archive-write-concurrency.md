# RIVERHOG_ARCHIVE_WRITE_CONCURRENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-write-concurrency:cf59eb9198 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-b200cddc47"></a>
| Field | Shape |
|---|---|
| <a id="s-9a409114ba"></a>`classification` | "runtime" |
| <a id="s-a64ea888cd"></a>`consumers` | ["riverhog-server"] |
| <a id="s-a6e0b99f6d"></a>`disposition` | "contractual" |
| <a id="s-114bfa5509"></a>`id` | "riverhog-server:environment:RIVERHOG_ARCHIVE_WRITE_CONCURRENCY" |
| <a id="s-5b43242159"></a>`name` | "RIVERHOG_ARCHIVE_WRITE_CONCURRENCY" |
| <a id="s-3435db812d"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_ARCHIVE_WRITE_CONCURRENCY"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_ARCHIVE_WRITE_CONCURRENCY](#s-b200cddc47) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-95b2b1ee11"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-66e76c5487"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_WRITE_CONCURRENCY](../../../evidence/sources.md#src-66ed403dd1) — `riverhog/src/riverhog_core/throughput.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/throughput.py` | `_env_int(values, 'RIVERHOG_ARCHIVE_WRITE_CONCURRENCY', DEFAULT_WRITE_CONCURRENCY)` |

### Machine authority

- `/external_contract/configuration_environment/41`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bd60f5137d1b5db213e83f5ccab745564808a40503180bc5c3186bdbe4a099c2 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_ARCHIVE_WRITE_CONCURRENCY",
  "name": "RIVERHOG_ARCHIVE_WRITE_CONCURRENCY",
  "owner": "riverhog-server"
}
```
