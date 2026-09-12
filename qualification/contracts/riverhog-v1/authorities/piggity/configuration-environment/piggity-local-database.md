# PIGGITY_LOCAL_DATABASE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:piggity:piggity-local-database:7571f69396 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-5718a6415e) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b221afcadd"></a>
| Field | Shape |
|---|---|
| <a id="s-305697c189"></a>`classification` | "identity" |
| <a id="s-81eb745f84"></a>`consumers` | ["piggity"] |
| <a id="s-4b2d0f8c27"></a>`disposition` | "contractual" |
| <a id="s-96466e0755"></a>`id` | "piggity:environment:PIGGITY_LOCAL_DATABASE" |
| <a id="s-9bc5608703"></a>`name` | "PIGGITY_LOCAL_DATABASE" |
| <a id="s-79238e712b"></a>`owner` | "piggity" |

## Governing policies

- <a id="pa-92c67f1c05"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:piggity:PIGGITY_LOCAL_DATABASE](../../../evidence/sources.md#src-ab1d0074de) — `reference/riverhog/applications/piggity/src/piggity/local.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/1/names` |
| parser | `piggity` | `reference/riverhog/applications/piggity/src/piggity/local.py` | `os.getenv('PIGGITY_LOCAL_DATABASE', '')` |

### Machine authority

- `/external_contract/configuration_environment/2`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8766b5ddda2dc8f70b4c33b80e7a371156813841951df0c22393ed3b2393b934 -->

```json
{
  "classification": "identity",
  "consumers": [
    "piggity"
  ],
  "disposition": "contractual",
  "id": "piggity:environment:PIGGITY_LOCAL_DATABASE",
  "name": "PIGGITY_LOCAL_DATABASE",
  "owner": "piggity"
}
```
