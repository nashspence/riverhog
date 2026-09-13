# stove0-review-target-support:configuration:review-target-config configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:stove0-review-target-support:stove0-review-target-support-configuratio-1de58493fa:7e049e6756 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [Configuration Documents](index.md) |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

<a id="s-d3e1aec762"></a>
- <a id="s-51615cd833"></a>`title`: ReviewTargetConfig
- <a id="s-4280543e3c"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1c32ef932b"></a>`samplers` | yes | type="array"; minItems=1; items=(#/$defs/SamplerConfig) |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-59188e1eee"></a>`SamplerConfig` | type="object"; fields=`allow_insecure_http`, `base_url`, `descriptor_sha256`, `id`, `image_digest`, `token_file`; additional keys=`additionalProperties`, `required` |

### Progression, limits, and lifecycle

#### [extent-rule/configuration-composition/v1](../../../policies/index.md#p-dcd344e8e5)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-review-target-support:configuration:review-target-config"}; maximum=null; reason="validated-deployment-composition"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field samplers](#s-1c32ef932b) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-1cfe63b462"></a>[definition SamplerConfig · field base_url](#s-59188e1eee) | `length · characters · contract_max` | maximum=2048; minimum=1; reason="schema-maximum" |
| <a id="s-377424141f"></a>[definition SamplerConfig · field descriptor_sha256](#s-59188e1eee) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-5f406f7876"></a>[definition SamplerConfig · field image_digest](#s-59188e1eee) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Governing policies

- <a id="pa-020c444442"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-091fe9ff09"></a>[extent-rule/configuration-composition/v1](../../../policies/index.md#p-dcd344e8e5)
- <a id="pa-57d941984f"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration:stove0-review-target-support:configuration:review-target-config](../../../evidence/sources.md#src-cca9387ce6) — `reference/stove0/targets/review/support/src/stove0_review_target_support/app.py::ReviewTargetConfig`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_documents/stove0-review-target-support:configuration:review-target-config`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 465ba4684267b5a403b655bcbaaa58dbe51e259b14ffa6dc2fbd902af816ff0c -->

```json
{
  "$defs": {
    "SamplerConfig": {
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
  },
  "additionalProperties": false,
  "properties": {
    "samplers": {
      "items": {
        "$ref": "#/$defs/SamplerConfig"
      },
      "minItems": 1,
      "title": "Samplers",
      "type": "array"
    }
  },
  "required": [
    "samplers"
  ],
  "title": "ReviewTargetConfig",
  "type": "object"
}
```
