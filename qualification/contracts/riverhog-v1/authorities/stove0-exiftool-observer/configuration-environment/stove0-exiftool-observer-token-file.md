# STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-exiftool-observer:stove0-exiftool-observer-token-file:7f77b98385 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-exiftool-observer](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [credential](index.md#f-369f4c14fa) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-74f1ce0aa1"></a>
| Field | Shape |
|---|---|
| <a id="s-09258eaf9a"></a>`classification` | "credential" |
| <a id="s-14c637bf7d"></a>`consumers` | ["stove0-exiftool-observer"] |
| <a id="s-8097a25c90"></a>`disposition` | "contractual" |
| <a id="s-5670701bbc"></a>`id` | "stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE" |
| <a id="s-9911339263"></a>`name` | "STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE" |
| <a id="s-a4a31c47b8"></a>`owner` | "stove0-exiftool-observer" |

## Governing policies

- <a id="pa-8246f8ac57"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE](../../../evidence/sources.md#src-7e8e1ce139) — `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/18/names` |
| parser | `stove0-exiftool-observer` | `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py` | `os.getenv('STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE')` |

### Machine authority

- `/external_contract/configuration_environment/91`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3801b193c9975314d920a53d1840fafb67b203022432159a5c04537cffadd78d -->

```json
{
  "classification": "credential",
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "disposition": "contractual",
  "id": "stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE",
  "name": "STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE",
  "owner": "stove0-exiftool-observer"
}
```
