# stove0-review-target-sampler configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:stove0-review-target-support:stove0-review-target-sampler-configuration:2b44439124 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [configuration](index.md) |
| Family | [documents](index.md#f-95b48814a9) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-450a429765"></a>
- <a id="s-caa3b9f29d"></a>`title`: SamplerConfig
- <a id="s-7f5dcbeaa6"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1c5ea150e1"></a>`allow_insecure_http` | no | type="boolean" |  |
| <a id="s-72f030d71c"></a>`base_url` | yes | type="string"; minLength=1; maxLength=2048 |  |
| <a id="s-8577e2ed53"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-dab0228633"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$" |  |
| <a id="s-2f182b21f9"></a>`image_digest` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-40399ebb2a"></a>`token_file` | yes | type="string"; format="path" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field base_url](#s-72f030d71c) | `length · characters · contract_max` | maximum=2048; minimum=1; reason="schema-maximum" |
| [field descriptor_sha256](#s-8577e2ed53) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field image_digest](#s-2f182b21f9) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Governing policies

- <a id="pa-584e9edf86"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-24c1e0859c"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration:stove0-review-target-sampler](../../../evidence/sources.md#src-2ef831d421) — `reference/stove0/targets/review/support/src/stove0_review_target_support/app.py::SamplerConfig`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_documents/stove0-review-target-sampler`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f41a6e541ec173a2e5d15cf28386e186b3440a8e7345c69f22509c6add3dad8e -->

```json
{
  "additionalProperties": false,
  "properties": {
    "allow_insecure_http": {
      "default": false,
      "title": "Allow Insecure Http",
      "type": "boolean"
    },
    "base_url": {
      "maxLength": 2048,
      "minLength": 1,
      "title": "Base Url",
      "type": "string"
    },
    "descriptor_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Descriptor Sha256",
      "type": "string"
    },
    "id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,118}[a-z0-9])?$",
      "title": "Id",
      "type": "string"
    },
    "image_digest": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Image Digest",
      "type": "string"
    },
    "token_file": {
      "format": "path",
      "title": "Token File",
      "type": "string"
    }
  },
  "required": [
    "id",
    "base_url",
    "token_file",
    "descriptor_sha256",
    "image_digest"
  ],
  "title": "SamplerConfig",
  "type": "object"
}
```
