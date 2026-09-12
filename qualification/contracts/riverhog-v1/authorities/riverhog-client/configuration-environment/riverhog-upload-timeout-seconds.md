# RIVERHOG_UPLOAD_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-upload-timeout-seconds:caa5b5e2bf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-e4b312fabf) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-c9bbf6ae4d"></a>
| Field | Shape |
|---|---|
| <a id="s-3d3ead85ef"></a>`classification` | "runtime" |
| <a id="s-f8e9201576"></a>`consumers` | ["riverhog-client"] |
| <a id="s-2c35a25ea8"></a>`disposition` | "contractual" |
| <a id="s-cf2890bc74"></a>`id` | "riverhog-client:environment:RIVERHOG_UPLOAD_TIMEOUT_SECONDS" |
| <a id="s-3ea82a14dd"></a>`name` | "RIVERHOG_UPLOAD_TIMEOUT_SECONDS" |
| <a id="s-12f813c67b"></a>`owner` | "riverhog-client" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_UPLOAD_TIMEOUT_SECONDS"; consumers=["riverhog-client"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_UPLOAD_TIMEOUT_SECONDS](#s-c9bbf6ae4d) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-4d59af46c0"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-4480e4ed51"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-client:RIVERHOG_UPLOAD_TIMEOUT_SECONDS](../../../evidence/sources.md#src-6e31cc159d) — `packages/riverhog-client/src/riverhog_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/5/names` |
| parser | `riverhog-client` | `packages/riverhog-client/src/riverhog_client/client.py` | `_timeout_seconds('RIVERHOG_UPLOAD_TIMEOUT_SECONDS', _UPLOAD_TIMEOUT_SECONDS)` |

### Machine authority

- `/external_contract/configuration_environment/20`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 650b6f33aa3d35b970762c42826955187a5778c4e6827443ac10d387b35bc00d -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-client"
  ],
  "disposition": "contractual",
  "id": "riverhog-client:environment:RIVERHOG_UPLOAD_TIMEOUT_SECONDS",
  "name": "RIVERHOG_UPLOAD_TIMEOUT_SECONDS",
  "owner": "riverhog-client"
}
```
