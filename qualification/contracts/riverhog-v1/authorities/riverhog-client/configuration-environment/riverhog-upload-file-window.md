# RIVERHOG_UPLOAD_FILE_WINDOW

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-upload-file-window:e5851d942e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-e4b312fabf) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-dc3b9e297a"></a>
| Field | Shape |
|---|---|
| <a id="s-12a3517adc"></a>`classification` | "runtime" |
| <a id="s-2cfcf2ca71"></a>`consumers` | ["riverhog-client"] |
| <a id="s-0cdf6596af"></a>`disposition` | "contractual" |
| <a id="s-57bcabee8d"></a>`id` | "riverhog-client:environment:RIVERHOG_UPLOAD_FILE_WINDOW" |
| <a id="s-3d0c74b453"></a>`name` | "RIVERHOG_UPLOAD_FILE_WINDOW" |
| <a id="s-90ff047361"></a>`owner` | "riverhog-client" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_UPLOAD_FILE_WINDOW"; consumers=["riverhog-client"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_UPLOAD_FILE_WINDOW](#s-dc3b9e297a) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-7fe83567fe"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-51003c1c83"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-client:RIVERHOG_UPLOAD_FILE_WINDOW](../../../evidence/sources.md#src-93e4082935) — `packages/riverhog-client/src/riverhog_client/uploads.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/5/names` |
| parser | `riverhog-client` | `packages/riverhog-client/src/riverhog_client/uploads.py` | `environment.get('RIVERHOG_UPLOAD_FILE_WINDOW', '')` |

### Machine authority

- `/external_contract/configuration_environment/19`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 043606c2b1ee704f8268e38cfa8e31e26cb1f1e25239df67cf3ba135e74e5613 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-client"
  ],
  "disposition": "contractual",
  "id": "riverhog-client:environment:RIVERHOG_UPLOAD_FILE_WINDOW",
  "name": "RIVERHOG_UPLOAD_FILE_WINDOW",
  "owner": "riverhog-client"
}
```
