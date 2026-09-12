# STOVE0_FFMPEG_BIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-nvenc-av1-opus-review-sampler:stove0-ffmpeg-bin:75f0bc97a3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-review-sampler](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-bab65f8aad) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-eb8f28adc2"></a>
| Field | Shape |
|---|---|
| <a id="s-0e3ceb1ea6"></a>`classification` | "identity" |
| <a id="s-f44dec2d22"></a>`consumers` | ["stove0-nvenc-av1-opus-review-sampler"] |
| <a id="s-17690047ae"></a>`disposition` | "contractual" |
| <a id="s-bf6317e26b"></a>`id` | "stove0-nvenc-av1-opus-review-sampler:environment:STOVE0_FFMPEG_BIN" |
| <a id="s-c875192905"></a>`name` | "STOVE0_FFMPEG_BIN" |
| <a id="s-9e801618d0"></a>`owner` | "stove0-nvenc-av1-opus-review-sampler" |

## Governing policies

- <a id="pa-ea14e20561"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-nvenc-av1-opus-review-sampler:STOVE0_FFMPEG_BIN](../../../evidence/sources.md#src-f77354177a) — `reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0_nvenc_av1_opus_review_sampler/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/22/names` |
| parser | `stove0-nvenc-av1-opus-review-sampler` | `reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0_nvenc_av1_opus_review_sampler/app.py` | `os.getenv('STOVE0_FFMPEG_BIN', 'ffmpeg')` |

### Machine authority

- `/external_contract/configuration_environment/101`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4485267a77a203486c942c58b09029a52f653c0d82503bddb9548b5239fe173e -->

```json
{
  "classification": "identity",
  "consumers": [
    "stove0-nvenc-av1-opus-review-sampler"
  ],
  "disposition": "contractual",
  "id": "stove0-nvenc-av1-opus-review-sampler:environment:STOVE0_FFMPEG_BIN",
  "name": "STOVE0_FFMPEG_BIN",
  "owner": "stove0-nvenc-av1-opus-review-sampler"
}
```
