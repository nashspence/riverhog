# RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-download-timeout-seconds:22678d4f8d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-e4b312fabf) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-11a73a67cc"></a>
| Field | Shape |
|---|---|
| <a id="s-ef6fbaebe3"></a>`classification` | "runtime" |
| <a id="s-56b56d915a"></a>`consumers` | ["riverhog-client"] |
| <a id="s-79743ce45b"></a>`disposition` | "contractual" |
| <a id="s-05a1ce1ce4"></a>`id` | "riverhog-client:environment:RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS" |
| <a id="s-514e7dfc94"></a>`name` | "RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS" |
| <a id="s-bba7a7ca74"></a>`owner` | "riverhog-client" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS"; consumers=["riverhog-client"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS](#s-11a73a67cc) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-724640aa4a"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-54f597582a"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-client:RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS](../../../evidence/sources.md#src-7d03d4b83f) — `packages/riverhog-client/src/riverhog_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/5/names` |
| parser | `riverhog-client` | `packages/riverhog-client/src/riverhog_client/client.py` | `_timeout_seconds('RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS', _DOWNLOAD_TIMEOUT_SECONDS)` |

### Machine authority

- `/external_contract/configuration_environment/13`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 43959cebd02589540f5841073acb1b449c991a220bd6b6382393d00431d9e1bb -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-client"
  ],
  "disposition": "contractual",
  "id": "riverhog-client:environment:RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS",
  "name": "RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS",
  "owner": "riverhog-client"
}
```
