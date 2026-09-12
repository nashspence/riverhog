# PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:piggity:piggity-upload-finalize-poll-seconds:fd59ef751b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-b2e9f036cd) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-2436bad7d4"></a>
| Field | Shape |
|---|---|
| <a id="s-42321b98d8"></a>`classification` | "runtime" |
| <a id="s-478f37a840"></a>`consumers` | ["piggity"] |
| <a id="s-5bf0005094"></a>`disposition` | "contractual" |
| <a id="s-7ed31707ad"></a>`id` | "piggity:environment:PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS" |
| <a id="s-7d2e712aed"></a>`name` | "PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS" |
| <a id="s-cfbebc15f6"></a>`owner` | "piggity" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS"; consumers=["piggity"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS](#s-2436bad7d4) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-63c0d1c141"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-2737a6a76e"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:piggity:PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS](../../../evidence/sources.md#src-2057e6d1cc) — `reference/riverhog/applications/piggity/src/piggity/main.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/2/names` |
| parser | `piggity` | `reference/riverhog/applications/piggity/src/piggity/main.py` | `os.getenv('PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS')` |

### Machine authority

- `/external_contract/configuration_environment/7`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 364a284afc1c04981948cd5122e243825eb08be0ce3d9bb4a3ec461d781ca7ea -->

```json
{
  "classification": "runtime",
  "consumers": [
    "piggity"
  ],
  "disposition": "contractual",
  "id": "piggity:environment:PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS",
  "name": "PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS",
  "owner": "piggity"
}
```
