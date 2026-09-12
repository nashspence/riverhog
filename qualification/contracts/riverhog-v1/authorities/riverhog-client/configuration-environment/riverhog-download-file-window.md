# RIVERHOG_DOWNLOAD_FILE_WINDOW

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-download-file-window:ffe76c129e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-e4b312fabf) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-b6148dfd81"></a>
| Field | Shape |
|---|---|
| <a id="s-31d865a90c"></a>`classification` | "runtime" |
| <a id="s-b49faf4a7f"></a>`consumers` | ["riverhog-client"] |
| <a id="s-dd86cd7700"></a>`disposition` | "contractual" |
| <a id="s-23f3a2f19a"></a>`id` | "riverhog-client:environment:RIVERHOG_DOWNLOAD_FILE_WINDOW" |
| <a id="s-a2dcbb91e6"></a>`name` | "RIVERHOG_DOWNLOAD_FILE_WINDOW" |
| <a id="s-392cf8391f"></a>`owner` | "riverhog-client" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_DOWNLOAD_FILE_WINDOW"; consumers=["riverhog-client"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_DOWNLOAD_FILE_WINDOW](#s-b6148dfd81) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-d3aa156fab"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-26f7542568"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-client:RIVERHOG_DOWNLOAD_FILE_WINDOW](../../../evidence/sources.md#src-4629108138) — `packages/riverhog-client/src/riverhog_client/downloads.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/5/names` |
| parser | `riverhog-client` | `packages/riverhog-client/src/riverhog_client/downloads.py` | `environment.get('RIVERHOG_DOWNLOAD_FILE_WINDOW', '')` |

### Machine authority

- `/external_contract/configuration_environment/12`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e4bcb0bb58ca924f80a0ba9aa441f6603b0ec94ee0f7b8ebfbf7ac0e6ebc8751 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-client"
  ],
  "disposition": "contractual",
  "id": "riverhog-client:environment:RIVERHOG_DOWNLOAD_FILE_WINDOW",
  "name": "RIVERHOG_DOWNLOAD_FILE_WINDOW",
  "owner": "riverhog-client"
}
```
