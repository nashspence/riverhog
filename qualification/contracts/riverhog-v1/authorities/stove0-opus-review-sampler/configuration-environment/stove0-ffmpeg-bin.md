# STOVE0_FFMPEG_BIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-opus-review-sampler:stove0-ffmpeg-bin:baa8e870f3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-review-sampler](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-38b9808bda) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-549c81fb7a"></a>
| Field | Shape |
|---|---|
| <a id="s-c93bcdd7f4"></a>`classification` | "identity" |
| <a id="s-da01a86256"></a>`consumers` | ["stove0-opus-review-sampler"] |
| <a id="s-47c5e1e14d"></a>`disposition` | "contractual" |
| <a id="s-b1b61e3fa9"></a>`id` | "stove0-opus-review-sampler:environment:STOVE0_FFMPEG_BIN" |
| <a id="s-dd46bf31d6"></a>`name` | "STOVE0_FFMPEG_BIN" |
| <a id="s-6bf4797208"></a>`owner` | "stove0-opus-review-sampler" |

## Governing policies

- <a id="pa-398ce3c1d1"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-opus-review-sampler:STOVE0_FFMPEG_BIN](../../../evidence/sources.md#src-194d028459) — `reference/stove0/targets/opus/review-sampler/src/stove0_opus_review_sampler/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/24/names` |
| parser | `stove0-opus-review-sampler` | `reference/stove0/targets/opus/review-sampler/src/stove0_opus_review_sampler/app.py` | `os.getenv('STOVE0_FFMPEG_BIN', 'ffmpeg')` |

### Machine authority

- `/external_contract/configuration_environment/104`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3435ff7e2a95557da3c3821f3a1158caa92e3967cd4e4fdac15a16a90e5d337e -->

```json
{
  "classification": "identity",
  "consumers": [
    "stove0-opus-review-sampler"
  ],
  "disposition": "contractual",
  "id": "stove0-opus-review-sampler:environment:STOVE0_FFMPEG_BIN",
  "name": "STOVE0_FFMPEG_BIN",
  "owner": "stove0-opus-review-sampler"
}
```
