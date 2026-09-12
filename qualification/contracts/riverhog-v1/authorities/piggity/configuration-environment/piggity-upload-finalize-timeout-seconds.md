# PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:piggity:piggity-upload-finalize-timeout-seconds:ff1f8afb32 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-b2e9f036cd) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-1966a8d0ec"></a>
| Field | Shape |
|---|---|
| <a id="s-88eb5b5a25"></a>`classification` | "runtime" |
| <a id="s-9a2311e6cd"></a>`consumers` | ["piggity"] |
| <a id="s-2e888d0743"></a>`disposition` | "contractual" |
| <a id="s-cf3af0a6fd"></a>`id` | "piggity:environment:PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS" |
| <a id="s-4341315209"></a>`name` | "PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS" |
| <a id="s-4ea39658ad"></a>`owner` | "piggity" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS"; consumers=["piggity"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS](#s-1966a8d0ec) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-aa85cdcd4e"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-b33ce227c4"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:piggity:PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS](../../../evidence/sources.md#src-40c863880d) — `reference/riverhog/applications/piggity/src/piggity/main.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/2/names` |
| parser | `piggity` | `reference/riverhog/applications/piggity/src/piggity/main.py` | `os.getenv('PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS')` |

### Machine authority

- `/external_contract/configuration_environment/8`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 00c30a8aeaefc6e816289cabf9bea577a195ddb9a62181dbe3424578321a8e4d -->

```json
{
  "classification": "runtime",
  "consumers": [
    "piggity"
  ],
  "disposition": "contractual",
  "id": "piggity:environment:PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS",
  "name": "PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS",
  "owner": "piggity"
}
```
