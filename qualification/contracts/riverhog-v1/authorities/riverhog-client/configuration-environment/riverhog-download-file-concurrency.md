# RIVERHOG_DOWNLOAD_FILE_CONCURRENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-download-file-concurrency:16dd490497 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-e4b312fabf) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-8f9926b885"></a>
| Field | Shape |
|---|---|
| <a id="s-c72512af4e"></a>`classification` | "runtime" |
| <a id="s-9172f4f7f0"></a>`consumers` | ["riverhog-client"] |
| <a id="s-58f6e2ff8e"></a>`disposition` | "contractual" |
| <a id="s-4e688baa7f"></a>`id` | "riverhog-client:environment:RIVERHOG_DOWNLOAD_FILE_CONCURRENCY" |
| <a id="s-1fd2f143d5"></a>`name` | "RIVERHOG_DOWNLOAD_FILE_CONCURRENCY" |
| <a id="s-42b278dcfa"></a>`owner` | "riverhog-client" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_DOWNLOAD_FILE_CONCURRENCY"; consumers=["riverhog-client"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_DOWNLOAD_FILE_CONCURRENCY](#s-8f9926b885) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-6616bb97e2"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-53e4e42afd"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-client:RIVERHOG_DOWNLOAD_FILE_CONCURRENCY](../../../evidence/sources.md#src-bb20193981) — `packages/riverhog-client/src/riverhog_client/downloads.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/5/names` |
| parser | `riverhog-client` | `packages/riverhog-client/src/riverhog_client/downloads.py` | `environment.get('RIVERHOG_DOWNLOAD_FILE_CONCURRENCY', '')` |

### Machine authority

- `/external_contract/configuration_environment/11`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 831a85d23414727557d733a20610c91c25b9ad2d68cdcd5cacfa3a7091f67c1f -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-client"
  ],
  "disposition": "contractual",
  "id": "riverhog-client:environment:RIVERHOG_DOWNLOAD_FILE_CONCURRENCY",
  "name": "RIVERHOG_DOWNLOAD_FILE_CONCURRENCY",
  "owner": "riverhog-client"
}
```
