# STOVE0_EXIFTOOL_OBSERVER_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-exiftool-observer:stove0-exiftool-observer-token:0edef37119 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-exiftool-observer](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [credential](index.md#f-369f4c14fa) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-f34a6099fd"></a>
| Field | Shape |
|---|---|
| <a id="s-c59ffc2787"></a>`classification` | "credential" |
| <a id="s-333cb6b464"></a>`consumers` | ["stove0-exiftool-observer"] |
| <a id="s-f97ddd2626"></a>`disposition` | "contractual" |
| <a id="s-e45db274e8"></a>`id` | "stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_TOKEN" |
| <a id="s-e8b92a27c1"></a>`name` | "STOVE0_EXIFTOOL_OBSERVER_TOKEN" |
| <a id="s-37c4cf2e50"></a>`owner` | "stove0-exiftool-observer" |

## Governing policies

- <a id="pa-94b6b87b6c"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_TOKEN](../../../evidence/sources.md#src-5cf4e00bd2) — `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/18/names` |
| parser | `stove0-exiftool-observer` | `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py` | `os.environ.pop('STOVE0_EXIFTOOL_OBSERVER_TOKEN')` |
| parser | `stove0-exiftool-observer` | `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py` | `os.getenv('STOVE0_EXIFTOOL_OBSERVER_TOKEN')` |

### Machine authority

- `/external_contract/configuration_environment/90`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 244cf760b72848752f0de3fd7939609f7f8d89c6f7e827edcbe1e811e0c32d6c -->

```json
{
  "classification": "credential",
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "disposition": "contractual",
  "id": "stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_TOKEN",
  "name": "STOVE0_EXIFTOOL_OBSERVER_TOKEN",
  "owner": "stove0-exiftool-observer"
}
```
