# STOVE0_EXIFTOOL_OBSERVER_IMAGE_DIGEST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-exiftool-observer:stove0-exiftool-observer-image-digest:5f33892bdc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-exiftool-observer](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-653370c949) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-41e02220a1"></a>
| Field | Shape |
|---|---|
| <a id="s-69b91b86e7"></a>`classification` | "identity" |
| <a id="s-84903db1b5"></a>`consumers` | ["stove0-exiftool-observer"] |
| <a id="s-f6debf4615"></a>`disposition` | "contractual" |
| <a id="s-db7c90b69a"></a>`id` | "stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_IMAGE_DIGEST" |
| <a id="s-f3010b390a"></a>`name` | "STOVE0_EXIFTOOL_OBSERVER_IMAGE_DIGEST" |
| <a id="s-8d1cbb3d58"></a>`owner` | "stove0-exiftool-observer" |

## Governing policies

- <a id="pa-1f07685ef3"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_IMAGE_DIGEST](../../../evidence/sources.md#src-e2f881a54b) — `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/19/names` |
| parser | `stove0-exiftool-observer` | `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py` | `os.getenv('STOVE0_EXIFTOOL_OBSERVER_IMAGE_DIGEST', '')` |

### Machine authority

- `/external_contract/configuration_environment/87`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0b4b57447d552522ef2c6784333632796413285f53aacf7e8f93e031f16dd967 -->

```json
{
  "classification": "identity",
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "disposition": "contractual",
  "id": "stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_IMAGE_DIGEST",
  "name": "STOVE0_EXIFTOOL_OBSERVER_IMAGE_DIGEST",
  "owner": "stove0-exiftool-observer"
}
```
