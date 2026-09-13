# STOVE0_OPUS_REVIEW_SAMPLER_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-opus-review-sampler:stove0-opus-review-sampler-token-file:65bed08d85 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-review-sampler](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-a88f06d1ec) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-af76c67541"></a>
| Field | Shape |
|---|---|
| <a id="s-bed40d38df"></a>`consumers` | ["stove0-opus-review-sampler"] |
| <a id="s-66ce145b7d"></a>`default_expressions` | ["unset"] |
| <a id="s-ccca75dedb"></a>`id` | "stove0-opus-review-sampler:environment:STOVE0_OPUS_REVIEW_SAMPLER_TOKEN_FILE" |
| <a id="s-3206dad185"></a>`input_shape` | "environment-string" |
| <a id="s-5dc06a10d9"></a>`name` | "STOVE0_OPUS_REVIEW_SAMPLER_TOKEN_FILE" |
| <a id="s-e81be50e39"></a>`owner` | "stove0-opus-review-sampler" |

## Governing policies

- <a id="pa-74cf61e233"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-opus-review-sampler:STOVE0_OPUS_REVIEW_SAMPLER_TOKEN_FILE](../../../evidence/sources.md#src-009b1d7ca5) — `reference/stove0/targets/opus/review-sampler/src/stove0_opus_review_sampler/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-opus-review-sampler` | `reference/stove0/targets/opus/review-sampler/src/stove0_opus_review_sampler/app.py` | `os.getenv(f'{prefix}_TOKEN_FILE')` |

### Machine authority

- `/external_contract/configuration_environment/188`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2531e09d6855dd495940cb75bf5df988403843bcc7c3329c920252768209f696 -->

```json
{
  "consumers": [
    "stove0-opus-review-sampler"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "stove0-opus-review-sampler:environment:STOVE0_OPUS_REVIEW_SAMPLER_TOKEN_FILE",
  "input_shape": "environment-string",
  "name": "STOVE0_OPUS_REVIEW_SAMPLER_TOKEN_FILE",
  "owner": "stove0-opus-review-sampler"
}
```
