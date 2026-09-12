# RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-upload-request-concurrency:ca0c3b0922 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-71ed3f559b"></a>
| Field | Shape |
|---|---|
| <a id="s-af8e979e94"></a>`classification` | "runtime" |
| <a id="s-93ede9eab6"></a>`consumers` | ["riverhog-server"] |
| <a id="s-580c9fccc9"></a>`disposition` | "contractual" |
| <a id="s-b5799a6efe"></a>`id` | "riverhog-server:environment:RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY" |
| <a id="s-a3d7c530e4"></a>`name` | "RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY" |
| <a id="s-23d7da2e59"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY](#s-71ed3f559b) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-653744d031"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-57c2e21bac"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY](../../../evidence/sources.md#src-b5ab0e168b) — `riverhog/src/riverhog_core/throughput.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/throughput.py` | `_env_int(values, 'RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY', DEFAULT_UPLOAD_REQUEST_CONCURRENCY)` |

### Machine authority

- `/external_contract/configuration_environment/39`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9da6e4ec8301907d3cae4bef480bb46a5a24c00b1fbd95ffb7afda5a45ecda84 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY",
  "name": "RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY",
  "owner": "riverhog-server"
}
```
