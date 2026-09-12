# stove0-review-target configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:stove0-review-target:stove0-review-target-configuration:39ebbd838e -->

| Audit field | Value |
|---|---|
| Authority | `stove0-review-target` |
| Interface | `configuration` |
| Family | `documents` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/configuration_documents/stove0-review-target`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configuration-composition/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `configuration:stove0-review-target` — `reference/stove0/targets/review/support/src/stove0_review_target_support/app.py::ReviewTargetConfig`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=2048, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |

## Contract summary

- `title`: ReviewTargetConfig
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `samplers` | yes | array |  |

### Definitions

| Definition | Shape |
|---|---|
| `SamplerConfig` | object |

## Complete owned contract

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
