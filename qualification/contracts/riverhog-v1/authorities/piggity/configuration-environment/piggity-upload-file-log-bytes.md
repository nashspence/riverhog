# PIGGITY_UPLOAD_FILE_LOG_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:piggity:piggity-upload-file-log-bytes:c8cbc86d7d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-b2e9f036cd) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-70ab20d602"></a>
| Field | Shape |
|---|---|
| <a id="s-3ec9f0afdb"></a>`classification` | "runtime" |
| <a id="s-cf5ca173ed"></a>`consumers` | ["piggity"] |
| <a id="s-4c6fa421bc"></a>`disposition` | "contractual" |
| <a id="s-3b7a45f265"></a>`id` | "piggity:environment:PIGGITY_UPLOAD_FILE_LOG_BYTES" |
| <a id="s-e26009ba61"></a>`name` | "PIGGITY_UPLOAD_FILE_LOG_BYTES" |
| <a id="s-ba143c7f61"></a>`owner` | "piggity" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="PIGGITY_UPLOAD_FILE_LOG_BYTES"; consumers=["piggity"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [PIGGITY_UPLOAD_FILE_LOG_BYTES](#s-70ab20d602) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-fbe9cb94c6"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-8e8a855fbf"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:piggity:PIGGITY_UPLOAD_FILE_LOG_BYTES](../../../evidence/sources.md#src-283d9ea8ad) — `reference/riverhog/applications/piggity/src/piggity/main.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/2/names` |
| parser | `piggity` | `reference/riverhog/applications/piggity/src/piggity/main.py` | `os.getenv('PIGGITY_UPLOAD_FILE_LOG_BYTES')` |

### Machine authority

- `/external_contract/configuration_environment/6`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8fad412136031885976ee73416b03f2ea33c13f46c836cb66da7cf1e29c2de11 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "piggity"
  ],
  "disposition": "contractual",
  "id": "piggity:environment:PIGGITY_UPLOAD_FILE_LOG_BYTES",
  "name": "PIGGITY_UPLOAD_FILE_LOG_BYTES",
  "owner": "piggity"
}
```
