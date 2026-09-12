# STOVE0_EXIFTOOL_BIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-exiftool-observer:stove0-exiftool-bin:570c2b8798 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-exiftool-observer](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-653370c949) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-8a57472b3d"></a>
| Field | Shape |
|---|---|
| <a id="s-32d87bb8cf"></a>`classification` | "identity" |
| <a id="s-8b87502075"></a>`consumers` | ["stove0-exiftool-observer"] |
| <a id="s-cd2a6086f7"></a>`disposition` | "contractual" |
| <a id="s-0d275a9d51"></a>`id` | "stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_BIN" |
| <a id="s-ebf779a82f"></a>`name` | "STOVE0_EXIFTOOL_BIN" |
| <a id="s-0db1e92c3c"></a>`owner` | "stove0-exiftool-observer" |

## Governing policies

- <a id="pa-b134abb7e2"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_BIN](../../../evidence/sources.md#src-db3b5f3ab1) — `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/19/names` |
| parser | `stove0-exiftool-observer` | `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py` | `os.getenv('STOVE0_EXIFTOOL_BIN', 'exiftool')` |

### Machine authority

- `/external_contract/configuration_environment/85`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 32514e6a7b9a31702dc880694358074362d949b3af0bc120b0f904088b758677 -->

```json
{
  "classification": "identity",
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "disposition": "contractual",
  "id": "stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_BIN",
  "name": "STOVE0_EXIFTOOL_BIN",
  "owner": "stove0-exiftool-observer"
}
```
