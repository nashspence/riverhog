# STOVE0_EXIFTOOL_OBSERVER_HOST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-exiftool-observer:stove0-exiftool-observer-host:ec0da2767b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-exiftool-observer](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-653370c949) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-284bbb4914"></a>
| Field | Shape |
|---|---|
| <a id="s-a6b0471152"></a>`classification` | "identity" |
| <a id="s-c9aa0936f8"></a>`consumers` | ["stove0-exiftool-observer"] |
| <a id="s-b1c219f8c7"></a>`disposition` | "contractual" |
| <a id="s-f91da4b941"></a>`id` | "stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_HOST" |
| <a id="s-9f2399bb48"></a>`name` | "STOVE0_EXIFTOOL_OBSERVER_HOST" |
| <a id="s-1932bf0a08"></a>`owner` | "stove0-exiftool-observer" |

## Governing policies

- <a id="pa-b0c4a56324"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_HOST](../../../evidence/sources.md#src-8dfcd7e460) — `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/19/names` |
| parser | `stove0-exiftool-observer` | `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py` | `os.getenv('STOVE0_EXIFTOOL_OBSERVER_HOST', '127.0.0.1')` |

### Machine authority

- `/external_contract/configuration_environment/86`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 463b0a0c673afeba8b9d7098067595701f1bf4ec2453acf099dbc1745962dbd2 -->

```json
{
  "classification": "identity",
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "disposition": "contractual",
  "id": "stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_HOST",
  "name": "STOVE0_EXIFTOOL_OBSERVER_HOST",
  "owner": "stove0-exiftool-observer"
}
```
