# STOVE0_EXIFTOOL_OBSERVER_WORKSPACE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-exiftool-observer:stove0-exiftool-observer-workspace:dfc2403df7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-exiftool-observer](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-653370c949) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-651129acd1"></a>
| Field | Shape |
|---|---|
| <a id="s-f215aed4e2"></a>`classification` | "identity" |
| <a id="s-0a8dd8ad5b"></a>`consumers` | ["stove0-exiftool-observer"] |
| <a id="s-0c73a45791"></a>`disposition` | "contractual" |
| <a id="s-448ec91883"></a>`id` | "stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_WORKSPACE" |
| <a id="s-b54b400817"></a>`name` | "STOVE0_EXIFTOOL_OBSERVER_WORKSPACE" |
| <a id="s-8ded7f4c58"></a>`owner` | "stove0-exiftool-observer" |

## Governing policies

- <a id="pa-f2049c4a95"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_WORKSPACE](../../../evidence/sources.md#src-98428ceea4) — `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/19/names` |
| parser | `stove0-exiftool-observer` | `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py` | `os.getenv('STOVE0_EXIFTOOL_OBSERVER_WORKSPACE', '/run/stove0-exiftool-observer')` |

### Machine authority

- `/external_contract/configuration_environment/92`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6e838fd16ce90b5dd1d15ab7ff007948470a945036d11df7000a9bb48d4cf795 -->

```json
{
  "classification": "identity",
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "disposition": "contractual",
  "id": "stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_WORKSPACE",
  "name": "STOVE0_EXIFTOOL_OBSERVER_WORKSPACE",
  "owner": "stove0-exiftool-observer"
}
```
