# generated:stove0-target: TargetDescriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:stove0-target-support:generated-stove0-target-targetdescriptor:5ff514394b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-6402a49c5a"></a>

- <a id="s-faa471dd1b"></a>`type`: `"object"`
- <a id="s-2808a50792"></a>`additionalProperties`: `false`
- <a id="s-69e2d072c7"></a>`required`: `["implementation_id","implementation_version","source_revision","image_digest","operations","descriptor_sha256"]`
- <a id="s-f875b74b73"></a>`title`: `"TargetDescriptor"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-eb8c99e012"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Descriptor Sha256" |  |
| <a id="s-071496ac40"></a>`image_digest` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Image Digest" |  |
| <a id="s-3db63a082c"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Implementation Id" |  |
| <a id="s-3650795828"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1; title="Implementation Version" |  |
| <a id="s-d53ed856de"></a>`operations` | yes | type="array"; items=([TargetOperationSupport](#s-8e45078ce6)); minItems=1; title="Operations" |  |
| <a id="s-b4d90de531"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1"; title="Protocol" |  |
| <a id="s-73dbeeee4c"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1; title="Source Revision" |  |
| <a id="s-f37815ab23"></a>`transport` | no | type="string"; const="riverhog-capability/v1"; default="riverhog-capability/v1"; title="Transport" |  |

### Definitions

- [JsonSchemaValidationProfile](#s-a1210ca405)
- [JsonValue](#s-245a220b5e)
- [TargetOperationSupport](#s-8e45078ce6)

### <a id="s-a1210ca405"></a>definition `JsonSchemaValidationProfile`

- <a id="s-752cf60b26"></a>`type`: `"object"`
- <a id="s-cc0aab3caa"></a>`additionalProperties`: `false`
- <a id="s-cb78f69649"></a>`required`: `["id","profile_sha256","schema"]`
- <a id="s-350d4dc21a"></a>`title`: `"JsonSchemaValidationProfile"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a07f0badb6"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema"; title="Dialect" |  |
| <a id="s-c51e221234"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only"; title="Format Policy" |  |
| <a id="s-efa057cafa"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-03198747a3"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-f155fb833c"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-245a220b5e)); title="Schema" |  |

### <a id="s-245a220b5e"></a>definition `JsonValue`

- Accepts: any JSON value.

### <a id="s-8e45078ce6"></a>definition `TargetOperationSupport`

- <a id="s-eb49ebf7b0"></a>`type`: `"object"`
- <a id="s-6377278156"></a>`additionalProperties`: `false`
- <a id="s-baf3d0ea64"></a>`required`: `["operation_id","operation_contract_sha256","options_schema"]`
- <a id="s-572831ea4c"></a>`title`: `"TargetOperationSupport"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dfa8e2a098"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-65a9101bbe"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Operation Id" |  |
| <a id="s-95fd6cdd64"></a>`options_schema` | yes | [JsonSchemaValidationProfile](#s-a1210ca405) |  |
| <a id="s-39724a9660"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection"; title="Result Kind" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-target-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field operations](#s-d53ed856de) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field descriptor_sha256](#s-eb8c99e012) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field image_digest](#s-071496ac40) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field implementation_version](#s-3650795828) | `length · characters · contract_max` | maximum=120; minimum=1; reason="schema-maximum" |
| [field source_revision](#s-73dbeeee4c) | `length · characters · contract_max` | maximum=200; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Related interface records

- [generated:stove0-target protocol](../process-protocol/generated-stove0-target-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-89cf33b80f"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-15996a6c47"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-1151c39033"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-target](../../../evidence/sources/authorities.md#src-2c42f9d39a) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/schemas.py::target\_schema\_bundle](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/schemas/TargetDescriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dfd0983e87f3bab1f450ebff6608457f15dc174a862d26bf2068e874be904e46 -->

```json
{
  "$defs": {
    "JsonSchemaValidationProfile": {
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
        "profile_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Profile Sha256",
          "type": "string"
        },
        "schema": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Schema",
          "type": "object"
        }
      },
      "required": [
        "id",
        "profile_sha256",
        "schema"
      ],
      "title": "JsonSchemaValidationProfile",
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
          "$ref": "#/$defs/JsonSchemaValidationProfile"
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
    "descriptor_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Descriptor Sha256",
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
    "descriptor_sha256"
  ],
  "title": "TargetDescriptor",
  "type": "object"
}
```

</details>
