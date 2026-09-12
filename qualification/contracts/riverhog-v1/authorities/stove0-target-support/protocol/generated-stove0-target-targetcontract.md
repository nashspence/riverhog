# generated:stove0-target: TargetContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-target-support:generated-stove0-target-targetcontract:5398dc2d99 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-3862b77c4ff3) |
| Contract elements | 1 |
| Extent decisions | 5 |

## External contract

<a id="s-f55c3c2305bb"></a>
- <a id="s-a9f5e1dd7166"></a>`title`: TargetContract
- <a id="s-7cc62d6bafb0"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ce89526554ca"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3d5d4b0d0589"></a>`image_digest` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7cea71058d19"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-701a84b78a04"></a>`implementation_version` | yes | type="string"; minLength=1; maxLength=120 |  |
| <a id="s-2211625ddb6c"></a>`operations` | yes | type="array"; minItems=1; items=(#/$defs/TargetOperationSupport) |  |
| <a id="s-cbff81d92c29"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"] |  |
| <a id="s-a7e1a14aeb30"></a>`source_revision` | yes | type="string"; minLength=1; maxLength=200 |  |
| <a id="s-6431073394ec"></a>`transport` | no | type="string"; const="riverhog-capability/v1" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-aeffe3dc52fa"></a>`JsonSchemaDocument` | type="object"; fields=`dialect`, `format_policy`, `id`, `schema`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-b290de24fa6e"></a>`JsonValue` | empty object |
| <a id="s-daec20e2d64e"></a>`TargetOperationSupport` | type="object"; fields=`operation_contract_sha256`, `operation_id`, `options_schema`, `result_kind`; additional keys=`additionalProperties`, `required` |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-target-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field operations](#s-2211625ddb6c) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field contract_sha256](#s-ce89526554ca) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field image_digest](#s-3d5d4b0d0589) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field implementation_version](#s-701a84b78a04) | `length · characters · contract_max` | maximum=120; minimum=1; reason="schema-maximum" |
| [field source_revision](#s-a7e1a14aeb30) | `length · characters · contract_max` | maximum=200; minimum=1; reason="schema-maximum" |

## Governing policies

- <a id="pa-ca7ee5f2d16f"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-e1c50527a2f7"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)
- <a id="pa-52379ffd6776"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:stove0-target](../../../evidence/sources.md#src-2c42f9d39a0b) — `reference/stove0/packages/target-support/src/stove0_target_support/schemas.py::target_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/schemas/TargetContract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 444d776c4e61c75ff1edfdfeac88aa1e2ca815679fc44af9eae1992e4dde5bf0 -->

```json
{
  "$defs": {
    "JsonSchemaDocument": {
      "additionalProperties": false,
      "properties": {
        "dialect": {
          "const": "https://json-schema.org/draft/2020-12/schema",
          "default": "https://json-schema.org/draft/2020-12/schema",
          "title": "Dialect",
          "type": "string"
        },
        "format_policy": {
          "const": "annotation-only",
          "default": "annotation-only",
          "title": "Format Policy",
          "type": "string"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "schema": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Schema",
          "type": "object"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Sha256",
          "type": "string"
        }
      },
      "required": [
        "id",
        "sha256",
        "schema"
      ],
      "title": "JsonSchemaDocument",
      "type": "object"
    },
    "JsonValue": {},
    "TargetOperationSupport": {
      "additionalProperties": false,
      "properties": {
        "operation_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Operation Contract Sha256",
          "type": "string"
        },
        "operation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Operation Id",
          "type": "string"
        },
        "options_schema": {
          "$ref": "#/$defs/JsonSchemaDocument"
        },
        "result_kind": {
          "default": "collection",
          "enum": [
            "collection",
            "external-effect"
          ],
          "title": "Result Kind",
          "type": "string"
        }
      },
      "required": [
        "operation_id",
        "operation_contract_sha256",
        "options_schema"
      ],
      "title": "TargetOperationSupport",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "contract_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Contract Sha256",
      "type": "string"
    },
    "image_digest": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Image Digest",
      "type": "string"
    },
    "implementation_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Implementation Id",
      "type": "string"
    },
    "implementation_version": {
      "maxLength": 120,
      "minLength": 1,
      "title": "Implementation Version",
      "type": "string"
    },
    "operations": {
      "items": {
        "$ref": "#/$defs/TargetOperationSupport"
      },
      "minItems": 1,
      "title": "Operations",
      "type": "array"
    },
    "protocol": {
      "default": "stove0-transform-target/v1",
      "enum": [
        "stove0-transform-target/v1",
        "stove0-effect-target/v1"
      ],
      "title": "Protocol",
      "type": "string"
    },
    "source_revision": {
      "maxLength": 200,
      "minLength": 1,
      "title": "Source Revision",
      "type": "string"
    },
    "transport": {
      "const": "riverhog-capability/v1",
      "default": "riverhog-capability/v1",
      "title": "Transport",
      "type": "string"
    }
  },
  "required": [
    "implementation_id",
    "implementation_version",
    "source_revision",
    "image_digest",
    "operations",
    "contract_sha256"
  ],
  "title": "TargetContract",
  "type": "object"
}
```
