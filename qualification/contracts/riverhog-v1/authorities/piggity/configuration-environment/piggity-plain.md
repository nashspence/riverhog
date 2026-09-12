# PIGGITY_PLAIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:piggity:piggity-plain:5da26ee4a9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-b2e9f036cd) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-e46514ac5a"></a>
| Field | Shape |
|---|---|
| <a id="s-30f987daa4"></a>`classification` | "runtime" |
| <a id="s-0a3f486fd1"></a>`consumers` | ["piggity"] |
| <a id="s-3ae8b437d2"></a>`disposition` | "contractual" |
| <a id="s-fff4744bb1"></a>`id` | "piggity:environment:PIGGITY_PLAIN" |
| <a id="s-92c12c2ce2"></a>`name` | "PIGGITY_PLAIN" |
| <a id="s-97c3efd577"></a>`owner` | "piggity" |

## Governing policies

- <a id="pa-788abf6f47"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:piggity:PIGGITY_PLAIN](../../../evidence/sources.md#src-88f78b70a7) — `reference/riverhog/applications/piggity/src/piggity/upload_progress.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/2/names` |
| parser | `piggity` | `reference/riverhog/applications/piggity/src/piggity/upload_progress.py` | `plain_output_requested('PIGGITY_PLAIN')` |

### Machine authority

- `/external_contract/configuration_environment/4`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c49c10b90acc7f7eb5b2f738652073234d37c40591d4094da12e03690435773f -->

```json
{
  "classification": "runtime",
  "consumers": [
    "piggity"
  ],
  "disposition": "contractual",
  "id": "piggity:environment:PIGGITY_PLAIN",
  "name": "PIGGITY_PLAIN",
  "owner": "piggity"
}
```
