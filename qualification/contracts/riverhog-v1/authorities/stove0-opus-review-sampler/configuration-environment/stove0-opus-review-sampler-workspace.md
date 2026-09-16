# STOVE0_OPUS_REVIEW_SAMPLER_WORKSPACE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-opus-review-sampler:stove0-opus-review-sampler-workspace:6efb6ea643 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-review-sampler](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-d930fc1ede"></a>

| Field | Value |
|---|---|
| <a id="s-31a8a30674"></a>`consumers` | `["stove0-opus-review-sampler"]` |
| <a id="s-bb8684b48d"></a>`default_expressions` | `["'/run/stove0-review'"]` |
| <a id="s-ddfe0f3dd5"></a>`id` | `"stove0-opus-review-sampler:environment:STOVE0_OPUS_REVIEW_SAMPLER_WORKSPACE"` |
| <a id="s-7e37cbb2be"></a>`input_shape` | `"environment-string"` |
| <a id="s-cb77dd8dad"></a>`name` | `"STOVE0_OPUS_REVIEW_SAMPLER_WORKSPACE"` |
| <a id="s-0393167173"></a>`owner` | `"stove0-opus-review-sampler"` |

## Governing policies

- <a id="pa-063b1c4093"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-opus-review-sampler:STOVE0_OPUS_REVIEW_SAMPLER_WORKSPACE](../../../evidence/sources.md#src-86f3eedb7a) — `reference/stove0/targets/opus/review-sampler/src/stove0_opus_review_sampler/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-opus-review-sampler` | `reference/stove0/targets/opus/review-sampler/src/stove0_opus_review_sampler/app.py` | `os.getenv(f'{prefix}_WORKSPACE', '/run/stove0-review')` |

### Machine authority

- `/external_contract/configuration_environment/189`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 331f89fe3929bacf8261cf4e5b8e649d897219da0072d6f35df9309dfc5db94b -->

```json
{
  "consumers": [
    "stove0-opus-review-sampler"
  ],
  "default_expressions": [
    "'/run/stove0-review'"
  ],
  "id": "stove0-opus-review-sampler:environment:STOVE0_OPUS_REVIEW_SAMPLER_WORKSPACE",
  "input_shape": "environment-string",
  "name": "STOVE0_OPUS_REVIEW_SAMPLER_WORKSPACE",
  "owner": "stove0-opus-review-sampler"
}
```

</details>
