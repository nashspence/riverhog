# generated:stove0-observer: ObserverDescriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:stove0-observer-support:generated-stove0-observer-observerdescriptor:6f2cb696df -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-f1f7699e24"></a>

- <a id="s-bb0c7ae8c9"></a>`type`: `"object"`
- <a id="s-08ddb3fe3c"></a>`additionalProperties`: `false`
- <a id="s-51c8a745ad"></a>`required`: `["implementation_id","implementation_version","source_revision","image_id","contracts","descriptor_sha256"]`
- <a id="s-91212768c6"></a>`title`: `"ObserverDescriptor"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-388af31281"></a>`contracts` | yes | type="array"; items=([ObserverContractSupport](#s-b516dd7564)); minItems=1; title="Contracts" |  |
| <a id="s-90ecbfd299"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Descriptor Sha256" |  |
| <a id="s-5e6b88d2f5"></a>`image_id` | yes | type="string"; pattern="^sha256:[0-9a-f]{64}$"; title="Image Id" |  |
| <a id="s-23bfd5c84f"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Implementation Id" |  |
| <a id="s-74afc1ae64"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1; title="Implementation Version" |  |
| <a id="s-56caddf497"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1"; title="Protocol" |  |
| <a id="s-906b91f7a3"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1; title="Source Revision" |  |

### Definitions

- [JsonSchemaValidationProfile](#s-3bfc104e13)
- [JsonValue](#s-ebc19bb186)
- [ObserverContractSupport](#s-b516dd7564)
- [SemanticValidationProfile](#s-39c220e195)

### <a id="s-3bfc104e13"></a>definition `JsonSchemaValidationProfile`

- <a id="s-97dde9cf89"></a>`type`: `"object"`
- <a id="s-df39c98fa4"></a>`additionalProperties`: `false`
- <a id="s-f5d302578d"></a>`required`: `["id","profile_sha256","schema"]`
- <a id="s-ca9f6d9caf"></a>`title`: `"JsonSchemaValidationProfile"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3b62aa4d01"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema"; title="Dialect" |  |
| <a id="s-8dde99dfd2"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only"; title="Format Policy" |  |
| <a id="s-7ff0ebf627"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-c705dd161f"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-1d07e97343"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-ebc19bb186)); title="Schema" |  |

### <a id="s-ebc19bb186"></a>definition `JsonValue`

- Accepts: any JSON value.

### <a id="s-b516dd7564"></a>definition `ObserverContractSupport`

- <a id="s-57639ad0ed"></a>`type`: `"object"`
- <a id="s-edffb3b2a8"></a>`additionalProperties`: `false`
- <a id="s-3808023f60"></a>`required`: `["contract_id","contract_sha256","options_schema","facts_schema","facts_semantics","maximum_result_bytes"]`
- <a id="s-49449c16c4"></a>`title`: `"ObserverContractSupport"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d6cc11d7fd"></a>`contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Contract Id" |  |
| <a id="s-32095f0bf9"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Contract Sha256" |  |
| <a id="s-32c625debc"></a>`facts_schema` | yes | [JsonSchemaValidationProfile](#s-3bfc104e13) |  |
| <a id="s-6001673b4a"></a>`facts_semantics` | yes | [SemanticValidationProfile](#s-39c220e195) |  |
| <a id="s-9240d02359"></a>`maximum_result_bytes` | yes | type="integer"; minimum=1; maximum=67108864; title="Maximum Result Bytes" |  |
| <a id="s-8bd48ae601"></a>`options_schema` | yes | [JsonSchemaValidationProfile](#s-3bfc104e13) |  |
| <a id="s-56b62e713f"></a>`preferred_subject_batch_size` | no | type="integer"; minimum=1; default=128; title="Preferred Subject Batch Size" |  |

### <a id="s-39c220e195"></a>definition `SemanticValidationProfile`

- <a id="s-d8d653cd92"></a>`type`: `"object"`
- <a id="s-71867298ac"></a>`additionalProperties`: `false`
- <a id="s-4e9e811324"></a>`required`: `["id","rules","profile_sha256"]`
- <a id="s-d0bf1523ab"></a>`title`: `"SemanticValidationProfile"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9c9e56304a"></a>`conformance_vectors_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Conformance Vectors Sha256" |  |
| <a id="s-6b99a16851"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-8f6364e24a"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-d46081cc2a"></a>`rules` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1; title="Rules" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-observer-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field contracts](#s-388af31281) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field descriptor_sha256](#s-90ecbfd299) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field implementation_version](#s-74afc1ae64) | `length · characters · contract_max` | maximum=120; minimum=1; reason="schema-maximum" |
| [field source_revision](#s-906b91f7a3) | `length · characters · contract_max` | maximum=200; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Related interface records

- [generated:stove0-observer protocol](../process-protocol/generated-stove0-observer-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-d9eedee78b"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-fb54bcd5a4"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-81237e0250"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-observer](../../../evidence/sources/authorities.md#src-dcc0b5485b) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/schemas.py::observer\_schema\_bundle](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-observer/schemas/ObserverDescriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fa658157c080298295557784e8455010ba93a7f7ce29f42ddbcb7bd370153003 -->

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
    "ObserverContractSupport": {
      "additionalProperties": false,
      "properties": {
        "contract_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Contract Id",
          "type": "string"
        },
        "contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Contract Sha256",
          "type": "string"
        },
        "facts_schema": {
          "$ref": "#/$defs/JsonSchemaValidationProfile"
        },
        "facts_semantics": {
          "$ref": "#/$defs/SemanticValidationProfile"
        },
        "maximum_result_bytes": {
          "maximum": 67108864,
          "minimum": 1,
          "title": "Maximum Result Bytes",
          "type": "integer"
        },
        "options_schema": {
          "$ref": "#/$defs/JsonSchemaValidationProfile"
        },
        "preferred_subject_batch_size": {
          "default": 128,
          "minimum": 1,
          "title": "Preferred Subject Batch Size",
          "type": "integer"
        }
      },
      "required": [
        "contract_id",
        "contract_sha256",
        "options_schema",
        "facts_schema",
        "facts_semantics",
        "maximum_result_bytes"
      ],
      "title": "ObserverContractSupport",
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
    "contracts": {
      "items": {
        "$ref": "#/$defs/ObserverContractSupport"
      },
      "minItems": 1,
      "title": "Contracts",
      "type": "array"
    },
    "descriptor_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Descriptor Sha256",
      "type": "string"
    },
    "image_id": {
      "pattern": "^sha256:[0-9a-f]{64}$",
      "title": "Image Id",
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
    "protocol": {
      "const": "stove0-content-observer/v1",
      "default": "stove0-content-observer/v1",
      "title": "Protocol",
      "type": "string"
    },
    "source_revision": {
      "maxLength": 200,
      "minLength": 1,
      "title": "Source Revision",
      "type": "string"
    }
  },
  "required": [
    "implementation_id",
    "implementation_version",
    "source_revision",
    "image_id",
    "contracts",
    "descriptor_sha256"
  ],
  "title": "ObserverDescriptor",
  "type": "object"
}
```

</details>
