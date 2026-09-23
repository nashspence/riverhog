# generated:stove0-target: TargetContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:stove0-target-support:generated-stove0-target-targetcontract:f43957b979 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-f55c3c2305"></a>

- <a id="s-7cc62d6baf"></a>`type`: `"object"`
- <a id="s-fe5042e887"></a>`additionalProperties`: `false`
- <a id="s-6cbf1e578a"></a>`required`: `["implementation_id","implementation_version","source_revision","image_digest","operations","contract_sha256"]`
- <a id="s-a9f5e1dd71"></a>`title`: `"TargetContract"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ce89526554"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Contract Sha256" |  |
| <a id="s-3d5d4b0d05"></a>`image_digest` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Image Digest" |  |
| <a id="s-7cea71058d"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Implementation Id" |  |
| <a id="s-701a84b78a"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1; title="Implementation Version" |  |
| <a id="s-2211625ddb"></a>`operations` | yes | type="array"; items=([TargetOperationSupport](#s-daec20e2d6)); minItems=1; title="Operations" |  |
| <a id="s-cbff81d92c"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1"; title="Protocol" |  |
| <a id="s-a7e1a14aeb"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1; title="Source Revision" |  |
| <a id="s-6431073394"></a>`transport` | no | type="string"; const="riverhog-capability/v1"; default="riverhog-capability/v1"; title="Transport" |  |

### Definitions

- [JsonSchemaDocument](#s-aeffe3dc52)
- [JsonValue](#s-b290de24fa)
- [TargetOperationSupport](#s-daec20e2d6)

### <a id="s-aeffe3dc52"></a>definition `JsonSchemaDocument`

- <a id="s-4290471eb7"></a>`type`: `"object"`
- <a id="s-f4f951a6a0"></a>`additionalProperties`: `false`
- <a id="s-eaad855815"></a>`required`: `["id","sha256","schema"]`
- <a id="s-350da25a62"></a>`title`: `"JsonSchemaDocument"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-72fca6c6ad"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema"; title="Dialect" |  |
| <a id="s-1fefb47f65"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only"; title="Format Policy" |  |
| <a id="s-33752d8cc4"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-a330577b00"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-b290de24fa)); title="Schema" |  |
| <a id="s-07d784ffcc"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### <a id="s-b290de24fa"></a>definition `JsonValue`

- Accepts: any JSON value.

### <a id="s-daec20e2d6"></a>definition `TargetOperationSupport`

- <a id="s-1019490372"></a>`type`: `"object"`
- <a id="s-9cb805cf4a"></a>`additionalProperties`: `false`
- <a id="s-cfe59ac913"></a>`required`: `["operation_id","operation_contract_sha256","options_schema"]`
- <a id="s-6601143406"></a>`title`: `"TargetOperationSupport"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-454f466e2c"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-4e03a2273e"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Operation Id" |  |
| <a id="s-ad9f4c1e3f"></a>`options_schema` | yes | [JsonSchemaDocument](#s-aeffe3dc52) |  |
| <a id="s-f6ae357d74"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection"; title="Result Kind" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-target-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field operations](#s-2211625ddb) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field contract_sha256](#s-ce89526554) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field image_digest](#s-3d5d4b0d05) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field implementation_version](#s-701a84b78a) | `length · characters · contract_max` | maximum=120; minimum=1; reason="schema-maximum" |
| [field source_revision](#s-a7e1a14aeb) | `length · characters · contract_max` | maximum=200; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Related interface records

- [generated:stove0-target protocol](../process-protocol/generated-stove0-target-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-733fa2641c"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-114d1f8aaa"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-617a3b9c02"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-target](../../../evidence/sources/authorities.md#src-2c42f9d39a) — [reference/stove0/packages/target-support/src/stove0\_target\_support/schemas.py::target\_schema\_bundle](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/schemas/TargetContract`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
