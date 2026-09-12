# PIGGITY_LOCAL_ROOT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:piggity:piggity-local-root:08698a98e6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-5718a6415e) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-bf86dd537b"></a>
| Field | Shape |
|---|---|
| <a id="s-5051f90947"></a>`classification` | "identity" |
| <a id="s-98d6331adc"></a>`consumers` | ["piggity"] |
| <a id="s-f518188ae9"></a>`disposition` | "contractual" |
| <a id="s-7ab9da7b1b"></a>`id` | "piggity:environment:PIGGITY_LOCAL_ROOT" |
| <a id="s-ffbf567bef"></a>`name` | "PIGGITY_LOCAL_ROOT" |
| <a id="s-8d1db45f77"></a>`owner` | "piggity" |

## Governing policies

- <a id="pa-fa21e0b90a"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:piggity:PIGGITY_LOCAL_ROOT](../../../evidence/sources.md#src-7c4314c2cb) — `reference/riverhog/applications/piggity/src/piggity/local.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/1/names` |
| parser | `piggity` | `reference/riverhog/applications/piggity/src/piggity/local.py` | `os.getenv('PIGGITY_LOCAL_ROOT', '')` |

### Machine authority

- `/external_contract/configuration_environment/3`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5374e148c1027e67e696399b9ae11740c80e71c7d4e59f6d7fe4b109895dbf72 -->

```json
{
  "classification": "identity",
  "consumers": [
    "piggity"
  ],
  "disposition": "contractual",
  "id": "piggity:environment:PIGGITY_LOCAL_ROOT",
  "name": "PIGGITY_LOCAL_ROOT",
  "owner": "piggity"
}
```
