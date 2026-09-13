# STOVE0_OPUS_REVIEW_SAMPLER_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-opus-review-sampler:stove0-opus-review-sampler-port:a730bd85a6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-review-sampler](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-556ef016c2"></a>
| Field | Shape |
|---|---|
| <a id="s-548453c111"></a>`consumers` | ["stove0-opus-review-sampler"] |
| <a id="s-99cea19f4f"></a>`default_expressions` | ["'8080'"] |
| <a id="s-d75baad09a"></a>`id` | "stove0-opus-review-sampler:environment:STOVE0_OPUS_REVIEW_SAMPLER_PORT" |
| <a id="s-83a8339220"></a>`input_shape` | "environment-string" |
| <a id="s-36b352e866"></a>`name` | "STOVE0_OPUS_REVIEW_SAMPLER_PORT" |
| <a id="s-5c39fe51b8"></a>`owner` | "stove0-opus-review-sampler" |

## Governing policies

- <a id="pa-ad2f807c15"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-opus-review-sampler:STOVE0_OPUS_REVIEW_SAMPLER_PORT](../../../evidence/sources.md#src-c80bc2cb9e) — `reference/stove0/targets/opus/review-sampler/src/stove0_opus_review_sampler/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-opus-review-sampler` | `reference/stove0/targets/opus/review-sampler/src/stove0_opus_review_sampler/app.py` | `os.getenv(f'{prefix}_PORT', '8080')` |

### Machine authority

- `/external_contract/configuration_environment/185`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 77a36a46a90d232f40054a8824329fa3ca882d090f6272ebd2bc269c853bca6c -->

```json
{
  "consumers": [
    "stove0-opus-review-sampler"
  ],
  "default_expressions": [
    "'8080'"
  ],
  "id": "stove0-opus-review-sampler:environment:STOVE0_OPUS_REVIEW_SAMPLER_PORT",
  "input_shape": "environment-string",
  "name": "STOVE0_OPUS_REVIEW_SAMPLER_PORT",
  "owner": "stove0-opus-review-sampler"
}
```
