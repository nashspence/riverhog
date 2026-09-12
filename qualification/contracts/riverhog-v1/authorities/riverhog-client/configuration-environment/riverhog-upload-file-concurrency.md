# RIVERHOG_UPLOAD_FILE_CONCURRENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-upload-file-concurrency:f4f58a730c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-e4b312fabf) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-ef80127a53"></a>
| Field | Shape |
|---|---|
| <a id="s-973a05fb7c"></a>`classification` | "runtime" |
| <a id="s-bf7f86f2e0"></a>`consumers` | ["riverhog-client"] |
| <a id="s-27a3f61b1d"></a>`disposition` | "contractual" |
| <a id="s-e8c3dd7148"></a>`id` | "riverhog-client:environment:RIVERHOG_UPLOAD_FILE_CONCURRENCY" |
| <a id="s-53140a7a23"></a>`name` | "RIVERHOG_UPLOAD_FILE_CONCURRENCY" |
| <a id="s-69c825bdc0"></a>`owner` | "riverhog-client" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_UPLOAD_FILE_CONCURRENCY"; consumers=["riverhog-client"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_UPLOAD_FILE_CONCURRENCY](#s-ef80127a53) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-323d97e3f1"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-b835273c2b"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-client:RIVERHOG_UPLOAD_FILE_CONCURRENCY](../../../evidence/sources.md#src-5d0adeaf5d) — `packages/riverhog-client/src/riverhog_client/uploads.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/5/names` |
| parser | `riverhog-client` | `packages/riverhog-client/src/riverhog_client/uploads.py` | `environment.get('RIVERHOG_UPLOAD_FILE_CONCURRENCY', '')` |

### Machine authority

- `/external_contract/configuration_environment/18`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1258b62dfdbf93c06d21d32decab7c3be1b32cc83baaecb0d910bb3f2db4b64f -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-client"
  ],
  "disposition": "contractual",
  "id": "riverhog-client:environment:RIVERHOG_UPLOAD_FILE_CONCURRENCY",
  "name": "RIVERHOG_UPLOAD_FILE_CONCURRENCY",
  "owner": "riverhog-client"
}
```
