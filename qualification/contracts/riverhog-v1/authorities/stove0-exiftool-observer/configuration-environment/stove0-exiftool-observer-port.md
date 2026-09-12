# STOVE0_EXIFTOOL_OBSERVER_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-exiftool-observer:stove0-exiftool-observer-port:cebed5fff3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-exiftool-observer](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-653370c949) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-5fe6983476"></a>
| Field | Shape |
|---|---|
| <a id="s-7966b1bd7e"></a>`classification` | "identity" |
| <a id="s-939e46bead"></a>`consumers` | ["stove0-exiftool-observer"] |
| <a id="s-dd3146b35b"></a>`disposition` | "contractual" |
| <a id="s-46a05f24bc"></a>`id` | "stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_PORT" |
| <a id="s-f7d158a894"></a>`name` | "STOVE0_EXIFTOOL_OBSERVER_PORT" |
| <a id="s-0050bf8e14"></a>`owner` | "stove0-exiftool-observer" |

## Governing policies

- <a id="pa-106fba92f2"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_PORT](../../../evidence/sources.md#src-ee07e99c6c) — `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/19/names` |
| parser | `stove0-exiftool-observer` | `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py` | `os.getenv('STOVE0_EXIFTOOL_OBSERVER_PORT', '8080')` |

### Machine authority

- `/external_contract/configuration_environment/88`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c013377baa6d631feb4eb15ed22fa84da4258158aa768f3bbebeb468850cd8a9 -->

```json
{
  "classification": "identity",
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "disposition": "contractual",
  "id": "stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_PORT",
  "name": "STOVE0_EXIFTOOL_OBSERVER_PORT",
  "owner": "stove0-exiftool-observer"
}
```
