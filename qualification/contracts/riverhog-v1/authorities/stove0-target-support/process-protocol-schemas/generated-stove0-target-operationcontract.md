# generated:stove0-target: OperationContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:stove0-target-support:generated-stove0-target-operationcontract:592bdf29d6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-eb1390484d"></a>

- <a id="s-cc7fe3474d"></a>`type`: `"object"`
- <a id="s-b8e70bf472"></a>`additionalProperties`: `false`
- <a id="s-d23df7a55a"></a>`required`: `["id","intent_schema","intent_semantics","inputs","contract_sha256"]`
- <a id="s-640878f7dc"></a>`title`: `"OperationContract"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d662de9b15"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Contract Sha256" |  |
| <a id="s-772086af5f"></a>`effect_receipt_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-8ce1551249)); (type="null")]; default=null |  |
| <a id="s-702fc5907b"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-f5e91f55d7"></a>`inputs` | yes | type="array"; items=([InputArtifactContract](#s-5f66949083)); minItems=1; title="Inputs" |  |
| <a id="s-51f7145810"></a>`intent_schema` | yes | [JsonSchemaValidationProfile](#s-8ce1551249) |  |
| <a id="s-7b1634eba7"></a>`intent_semantics` | yes | [SemanticValidationProfile](#s-c4cc0dce83) |  |
| <a id="s-b55e379a61"></a>`outputs` | no | type="array"; default=[]; items=([OutputArtifactContract](#s-63772a2004)); title="Outputs" |  |
| <a id="s-fc20921336"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection"; title="Result Kind" |  |
| <a id="s-09f0f21d9f"></a>`source_retirement_permitted` | no | type="boolean"; default=false; title="Source Retirement Permitted" |  |

### Definitions

- [InputArtifactContract](#s-5f66949083)
- [JsonSchemaValidationProfile](#s-8ce1551249)
- [JsonValue](#s-003a178d94)
- [OutputArtifactContract](#s-63772a2004)
- [SemanticValidationProfile](#s-c4cc0dce83)

### <a id="s-5f66949083"></a>definition `InputArtifactContract`

- <a id="s-e9f5fa2f9f"></a>`type`: `"object"`
- <a id="s-46366ddc09"></a>`additionalProperties`: `false`
- <a id="s-f9ce335da7"></a>`required`: `["role"]`
- <a id="s-b41c397e4c"></a>`title`: `"InputArtifactContract"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d1ffbc5970"></a>`allowed_dispositions` | no | anyOf=[(type="array"; items=(type="string"; enum=["transformed","preserved","omitted","rejected"])); (type="null")]; default=null; title="Allowed Dispositions" |  |
| <a id="s-5eb9ef1041"></a>`maximum` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; default=null; title="Maximum" |  |
| <a id="s-873a544617"></a>`minimum` | no | type="integer"; minimum=0; default=1; title="Minimum" |  |
| <a id="s-a65c58876b"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |

### <a id="s-8ce1551249"></a>definition `JsonSchemaValidationProfile`

- <a id="s-139e56e6d9"></a>`type`: `"object"`
- <a id="s-b8c44341a8"></a>`additionalProperties`: `false`
- <a id="s-a5208648a9"></a>`required`: `["id","profile_sha256","schema"]`
- <a id="s-b666858977"></a>`title`: `"JsonSchemaValidationProfile"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-63c3e09422"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema"; title="Dialect" |  |
| <a id="s-731e820882"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only"; title="Format Policy" |  |
| <a id="s-b06c618a91"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-d9b3640689"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-07dd739bdb"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-003a178d94)); title="Schema" |  |

### <a id="s-003a178d94"></a>definition `JsonValue`

- Accepts: any JSON value.

### <a id="s-63772a2004"></a>definition `OutputArtifactContract`

- <a id="s-2b61499867"></a>`type`: `"object"`
- <a id="s-f18e991bf6"></a>`additionalProperties`: `false`
- <a id="s-bbaab1ab4e"></a>`required`: `["role","derived_from_roles"]`
- <a id="s-9151f143b1"></a>`title`: `"OutputArtifactContract"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-db0de128fe"></a>`derived_from_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1; title="Derived From Roles" |  |
| <a id="s-227d3712af"></a>`maximum` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; default=null; title="Maximum" |  |
| <a id="s-7ce7d9330f"></a>`minimum` | no | type="integer"; minimum=0; default=1; title="Minimum" |  |
| <a id="s-2f7d54ac6d"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |

### <a id="s-c4cc0dce83"></a>definition `SemanticValidationProfile`

- <a id="s-2044452b76"></a>`type`: `"object"`
- <a id="s-c0972e15ff"></a>`additionalProperties`: `false`
- <a id="s-db27ef17c4"></a>`required`: `["id","rules","profile_sha256"]`
- <a id="s-7e230c23ce"></a>`title`: `"SemanticValidationProfile"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a0b3e86781"></a>`conformance_vectors_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Conformance Vectors Sha256" |  |
| <a id="s-10a57fc590"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-133a5bb76e"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-2f2dbe7f7f"></a>`rules` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1; title="Rules" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-target-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field inputs](#s-f5e91f55d7) | `cardinality · items · operational_policy` | shared above |
| [field outputs](#s-b55e379a61) | `cardinality · items · operational_policy` | shared above |
| <a id="s-52aee30098"></a>[definition InputArtifactContract · field allowed_dispositions · array value](#s-d1ffbc5970) | `cardinality · items · operational_policy` | shared above |
| [definition JsonSchemaValidationProfile · field schema](#s-07dd739bdb) | `cardinality · entries · operational_policy` | shared above |
| [definition OutputArtifactContract · field derived_from_roles](#s-db0de128fe) | `cardinality · items · operational_policy` | shared above |
| [definition SemanticValidationProfile · field rules](#s-2f2dbe7f7f) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field contract_sha256](#s-d662de9b15) | `length · characters · fixed` | shared above |
| [definition JsonSchemaValidationProfile · field profile_sha256](#s-d9b3640689) | `length · characters · fixed` | shared above |
| <a id="s-4052e4cd9d"></a>[definition SemanticValidationProfile · field conformance_vectors_sha256 · string value](#s-a0b3e86781) | `length · characters · fixed` | shared above |
| [definition SemanticValidationProfile · field profile_sha256](#s-133a5bb76e) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [generated:stove0-target protocol](../process-protocol/generated-stove0-target-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-018bb7213f"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-31d61071b2"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-a7cca2a7b5"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-target](../../../evidence/sources/authorities.md#src-2c42f9d39a) — [reference/stove0/packages/target-support/src/stove0\_target\_support/schemas.py::target\_schema\_bundle](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/schemas/OperationContract`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 55be6abb3d3e6881cba731f8ea8c857355d44523fa90994f5c880376bae376aa -->

```json
{
  "$defs": {
    "InputArtifactContract": {
      "additionalProperties": false,
      "properties": {
        "allowed_dispositions": {
          "anyOf": [
            {
              "items": {
                "enum": [
                  "transformed",
                  "preserved",
                  "omitted",
                  "rejected"
                ],
                "type": "string"
              },
              "type": "array"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Allowed Dispositions"
        },
        "maximum": {
          "anyOf": [
            {
              "minimum": 1,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Maximum"
        },
        "minimum": {
          "default": 1,
          "minimum": 0,
          "title": "Minimum",
          "type": "integer"
        },
        "role": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Role",
          "type": "string"
        }
      },
      "required": [
        "role"
      ],
      "title": "InputArtifactContract",
      "type": "object"
    },
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
    "OutputArtifactContract": {
      "additionalProperties": false,
      "properties": {
        "derived_from_roles": {
          "items": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "type": "string"
          },
          "minItems": 1,
          "title": "Derived From Roles",
          "type": "array"
        },
        "maximum": {
          "anyOf": [
            {
              "minimum": 1,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Maximum"
        },
        "minimum": {
          "default": 1,
          "minimum": 0,
          "title": "Minimum",
          "type": "integer"
        },
        "role": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Role",
          "type": "string"
        }
      },
      "required": [
        "role",
        "derived_from_roles"
      ],
      "title": "OutputArtifactContract",
      "type": "object"
    },
    "SemanticValidationProfile": {
      "additionalProperties": false,
      "properties": {
        "conformance_vectors_sha256": {
          "anyOf": [
            {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Conformance Vectors Sha256"
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
        "rules": {
          "items": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "type": "string"
          },
          "minItems": 1,
          "title": "Rules",
          "type": "array"
        }
      },
      "required": [
        "id",
        "rules",
        "profile_sha256"
      ],
      "title": "SemanticValidationProfile",
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
    "effect_receipt_schema": {
      "anyOf": [
        {
          "$ref": "#/$defs/JsonSchemaValidationProfile"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Id",
      "type": "string"
    },
    "inputs": {
      "items": {
        "$ref": "#/$defs/InputArtifactContract"
      },
      "minItems": 1,
      "title": "Inputs",
      "type": "array"
    },
    "intent_schema": {
      "$ref": "#/$defs/JsonSchemaValidationProfile"
    },
    "intent_semantics": {
      "$ref": "#/$defs/SemanticValidationProfile"
    },
    "outputs": {
      "default": [],
      "items": {
        "$ref": "#/$defs/OutputArtifactContract"
      },
      "title": "Outputs",
      "type": "array"
    },
    "result_kind": {
      "default": "collection",
      "enum": [
        "collection",
        "external-effect"
      ],
      "title": "Result Kind",
      "type": "string"
    },
    "source_retirement_permitted": {
      "default": false,
      "title": "Source Retirement Permitted",
      "type": "boolean"
    }
  },
  "required": [
    "id",
    "intent_schema",
    "intent_semantics",
    "inputs",
    "contract_sha256"
  ],
  "title": "OperationContract",
  "type": "object"
}
```

</details>
