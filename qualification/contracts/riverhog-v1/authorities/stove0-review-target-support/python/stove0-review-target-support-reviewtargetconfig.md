# stove0_review_target_support.ReviewTargetConfig

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-support:stove0-review-target-support-reviewtargetconfig:de19460673 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e55fe2bc05"></a>
- <a id="s-e72f049f29"></a>`distribution`: `stove0-review-target-support`
- <a id="s-b74952cfa1"></a>`module`: `stove0_review_target_support`
- <a id="s-6b0b03ea44"></a>`name`: `ReviewTargetConfig`
- <a id="s-38ac975c44"></a>`unit`: `export`

### Declared structure

- <a id="s-d5db7b461b"></a>`kind`: `"class"`
- <a id="s-60b99e18f4"></a>`signature`: `"'(*, samplers: Annotated[tuple[stove0_review_target_support.app.SamplerConfig, ...], MinLen(min_length=1)]) -> None'"`

#### Validated model schema

<a id="s-243034401d"></a>
- <a id="s-261a0d1b65"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-849c1b0a82"></a>`samplers` | yes | type="array"; minItems=1; items=(#/$defs/SamplerConfig) |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-50d7f1f942"></a>`SamplerConfig` | type="object"; fields=`allow_insecure_http`, `base_url`, `descriptor_sha256`, `id`, `image_digest`, `token_file`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [canonical_samplers](stove0-review-target-support-reviewtargetconfig-canonical-samplers.md)

## Governing policies

- <a id="pa-1c1985e416"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-support:stove0_review_target_support](../../../evidence/sources.md#src-2a89a71c41) — `reference/stove0/targets/review/support/src/stove0_review_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_support.ReviewTargetConfig`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 34faf56d0bbf7d09a368f5cb18343f5febf07a45f33ada51f8567d298206c6dc -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "SamplerConfig": {
          "additionalProperties": false,
          "properties": {
            "allow_insecure_http": {
              "default": false,
              "type": "boolean"
            },
            "base_url": {
              "maxLength": 2048,
              "minLength": 1,
              "type": "string"
            },
            "descriptor_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,118}[a-z0-9])?$",
              "type": "string"
            },
            "image_digest": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "token_file": {
              "format": "path",
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
          "type": "array"
        }
      },
      "required": [
        "samplers"
      ],
      "type": "object"
    },
    "signature": "'(*, samplers: Annotated[tuple[stove0_review_target_support.app.SamplerConfig, ...], MinLen(min_length=1)]) -> None'"
  },
  "distribution": "stove0-review-target-support",
  "module": "stove0_review_target_support",
  "name": "ReviewTargetConfig",
  "unit": "export"
}
```
