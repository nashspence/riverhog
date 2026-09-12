# STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-exiftool-observer:stove0-exiftool-observer-source-revision:16440c2821 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-exiftool-observer](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-653370c949) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-3a00485a0d"></a>
| Field | Shape |
|---|---|
| <a id="s-e2815c58dd"></a>`classification` | "identity" |
| <a id="s-f4162f50d7"></a>`consumers` | ["stove0-exiftool-observer"] |
| <a id="s-35d6cf5067"></a>`disposition` | "contractual" |
| <a id="s-c084c603a4"></a>`id` | "stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION" |
| <a id="s-811edc1b84"></a>`name` | "STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION" |
| <a id="s-a77e047c01"></a>`owner` | "stove0-exiftool-observer" |

## Governing policies

- <a id="pa-8efb82f7a2"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION](../../../evidence/sources.md#src-29eef6aa4c) — `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/19/names` |
| parser | `stove0-exiftool-observer` | `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py` | `os.getenv('STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION', 'unknown')` |

### Machine authority

- `/external_contract/configuration_environment/89`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b9b409cc5d2f68b1b1a5faf2b3a29885ffcaf69b371a4e9adef95a444aec9e19 -->

```json
{
  "classification": "identity",
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "disposition": "contractual",
  "id": "stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION",
  "name": "STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION",
  "owner": "stove0-exiftool-observer"
}
```
