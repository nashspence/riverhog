# stove0-review-target configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:stove0-review-target-support:stove0-review-target-configuration:9e1a5f76b5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [configuration](index.md) |
| Family | [documents](index.md#f-95b48814a9) |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

<a id="s-861ef5dbc3"></a>
- <a id="s-6b3d5a83f1"></a>`title`: ReviewTargetConfig
- <a id="s-3dc7056724"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e4b0b39689"></a>`samplers` | yes | type="array"; minItems=1; items=(#/$defs/SamplerConfig) |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-3c4d0d9bc8"></a>`SamplerConfig` | type="object"; fields=`allow_insecure_http`, `base_url`, `descriptor_sha256`, `id`, `image_digest`, `token_file`; additional keys=`additionalProperties`, `required` |

### Progression, limits, and lifecycle

#### [extent-rule/configuration-composition/v1](../../../policies/index.md#p-dcd344e8e5)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-review-target"}; maximum=null; reason="validated-deployment-composition"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field samplers](#s-e4b0b39689) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-6a8e1c233d"></a>[definition SamplerConfig · field base_url](#s-3c4d0d9bc8) | `length · characters · contract_max` | maximum=2048; minimum=1; reason="schema-maximum" |
| <a id="s-bbbbf5eca6"></a>[definition SamplerConfig · field descriptor_sha256](#s-3c4d0d9bc8) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-610bd7e599"></a>[definition SamplerConfig · field image_digest](#s-3c4d0d9bc8) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Governing policies

- <a id="pa-daa5617b3e"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-2694ccec88"></a>[extent-rule/configuration-composition/v1](../../../policies/index.md#p-dcd344e8e5)
- <a id="pa-1a9165f2ab"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration:stove0-review-target](../../../evidence/sources.md#src-d3261b60a1) — `reference/stove0/targets/review/support/src/stove0_review_target_support/app.py::ReviewTargetConfig`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_documents/stove0-review-target`

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
