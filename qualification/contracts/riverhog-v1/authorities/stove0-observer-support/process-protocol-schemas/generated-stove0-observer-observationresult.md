# generated:stove0-observer: ObservationResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:stove0-observer-support:generated-stove0-observer-observationresult:c040849c7d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-3e069889fe"></a>

- <a id="s-be967c2800"></a>`type`: `"object"`
- <a id="s-fc47f72448"></a>`additionalProperties`: `false`
- <a id="s-86f42031c2"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`
- <a id="s-0915193d16"></a>`title`: `"ObservationResult"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4885381720"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-cedb39dbf0)); title="Execution Evidence" |  |
| <a id="s-f8f907047c"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-cedb39dbf0))); (type="null")]; default=null; title="Facts" |  |
| <a id="s-6ba826046e"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-97d6727ced)); (type="null")]; default=null |  |
| <a id="s-8c37ff6dcf"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Facts Sha256" |  |
| <a id="s-d5175b296b"></a>`failure` | no | anyOf=[([ObservationFailure](#s-0d349ef758)); (type="null")]; default=null |  |
| <a id="s-b3503f6e3f"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1"; title="Format" |  |
| <a id="s-4f119c8082"></a>`inapplicable` | no | anyOf=[([ObservationInapplicable](#s-020dd70e7f)); (type="null")]; default=null |  |
| <a id="s-eab256d346"></a>`observer` | yes | [ObserverImplementation](#s-c0e8f13fc1) |  |
| <a id="s-63bf18ad99"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Observer Contract Id" |  |
| <a id="s-1d1b25fd9e"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Contract Sha256" |  |
| <a id="s-6109c79a68"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Id" |  |
| <a id="s-d3d591bbf9"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Result Sha256" |  |
| <a id="s-9ccfb25431"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"]; title="State" |  |
| <a id="s-4cedca4334"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-102b064907)); minItems=1; title="Subjects" |  |

### Definitions

- [ArtifactSubject](#s-102b064907)
- [CollectionId](#s-12b57ce892)
- [CollectionRootRef](#s-11fe04c507)
- [JsonSchemaValidationProfile](#s-97d6727ced)
- [JsonValue](#s-cedb39dbf0)
- [ObservationFailure](#s-0d349ef758)
- [ObservationInapplicable](#s-020dd70e7f)
- [ObserverImplementation](#s-c0e8f13fc1)

### <a id="s-102b064907"></a>definition `ArtifactSubject`

- <a id="s-33bd5eaa78"></a>`type`: `"object"`
- <a id="s-d0a4eb90b1"></a>`additionalProperties`: `false`
- <a id="s-55c8a30eaa"></a>`required`: `["id","role","collection","path","bytes","sha256"]`
- <a id="s-e7126d9e68"></a>`title`: `"ArtifactSubject"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-eed3c6ed8e"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-4397369082"></a>`collection` | yes | [CollectionRootRef](#s-11fe04c507) |  |
| <a id="s-b5d7cecab4"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-32e45d6d47"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null; title="Media Type" |  |
| <a id="s-034de74546"></a>`path` | yes | type="string"; maxLength=4096; minLength=1; title="Path" |  |
| <a id="s-5cb1ed1f8e"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |
| <a id="s-fccf11dcc3"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### <a id="s-12b57ce892"></a>definition `CollectionId`


#### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-ccac689f7a"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-70177c561f"></a>2 | not=(const="0") |

### <a id="s-11fe04c507"></a>definition `CollectionRootRef`

- <a id="s-252d1df4e8"></a>`type`: `"object"`
- <a id="s-584aabf2bd"></a>`additionalProperties`: `false`
- <a id="s-0234d04067"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`
- <a id="s-61a214bee1"></a>`title`: `"CollectionRootRef"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8081fae60c"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Archive Root Sha256" |  |
| <a id="s-6add64b493"></a>`collection_id` | yes | [CollectionId](#s-12b57ce892) |  |
| <a id="s-90a6daa640"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Content Identity" |  |

### <a id="s-97d6727ced"></a>definition `JsonSchemaValidationProfile`

- <a id="s-ccb9f2df2e"></a>`type`: `"object"`
- <a id="s-7704fc27ae"></a>`additionalProperties`: `false`
- <a id="s-c13c37c4d6"></a>`required`: `["id","profile_sha256","schema"]`
- <a id="s-e163e069ac"></a>`title`: `"JsonSchemaValidationProfile"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f1e5ea5e9a"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema"; title="Dialect" |  |
| <a id="s-8cb54c4c35"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only"; title="Format Policy" |  |
| <a id="s-a79f8783af"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-0b91c68702"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-60ca23fd27"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-cedb39dbf0)); title="Schema" |  |

### <a id="s-cedb39dbf0"></a>definition `JsonValue`

- Accepts: any JSON value.

### <a id="s-0d349ef758"></a>definition `ObservationFailure`

- <a id="s-1566b4bdab"></a>`type`: `"object"`
- <a id="s-8dcb0e8df7"></a>`additionalProperties`: `false`
- <a id="s-3d080c8753"></a>`required`: `["code","message","retryable"]`
- <a id="s-195b2386ab"></a>`title`: `"ObservationFailure"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-74847af185"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-478ba5fee9"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |
| <a id="s-7f525ed5e3"></a>`retryable` | yes | type="boolean"; title="Retryable" |  |

### <a id="s-020dd70e7f"></a>definition `ObservationInapplicable`

- <a id="s-8ef90c8b5b"></a>`type`: `"object"`
- <a id="s-69d718e658"></a>`additionalProperties`: `false`
- <a id="s-e52527024b"></a>`required`: `["code","message"]`
- <a id="s-e051be9fc0"></a>`title`: `"ObservationInapplicable"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6bd4d1fea1"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-6649271723"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |

### <a id="s-c0e8f13fc1"></a>definition `ObserverImplementation`

- <a id="s-013154b046"></a>`type`: `"object"`
- <a id="s-8fafb0ca1a"></a>`additionalProperties`: `false`
- <a id="s-dc7a4e94a9"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`
- <a id="s-9f38e106ed"></a>`title`: `"ObserverImplementation"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-81356e3af5"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Descriptor Sha256" |  |
| <a id="s-f57ebee6d8"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-9db62798d2"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1"; title="Protocol" |  |
| <a id="s-d0ce534807"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1; title="Source Revision" |  |
| <a id="s-2cb70f1d41"></a>`version` | yes | type="string"; maxLength=120; minLength=1; title="Version" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-observer-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field execution_evidence](#s-4885381720) | `cardinality · entries · operational_policy` | shared above |
| <a id="s-f8e15caec1"></a>[field facts · object value](#s-f8f907047c) | `cardinality · entries · operational_policy` | shared above |
| [field subjects](#s-4cedca4334) | `cardinality · items · operational_policy` | shared above |
| [definition JsonSchemaValidationProfile · field schema](#s-60ca23fd27) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-23a9f4e568"></a>[field facts_sha256 · string value](#s-8c37ff6dcf) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field observer_contract_sha256](#s-1d1b25fd9e) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field request_id](#s-6109c79a68) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field result_sha256](#s-d3d591bbf9) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition JsonSchemaValidationProfile · field profile_sha256](#s-0b91c68702) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObservationFailure · field message](#s-478ba5fee9) | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |
| [definition ObservationInapplicable · field message](#s-6649271723) | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |
| [definition ObserverImplementation · field descriptor_sha256](#s-81356e3af5) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObserverImplementation · field source_revision](#s-d0ce534807) | `length · characters · contract_max` | maximum=200; minimum=1; reason="schema-maximum" |
| [definition ObserverImplementation · field version](#s-2cb70f1d41) | `length · characters · contract_max` | maximum=120; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Related interface records

- [generated:stove0-observer protocol](../process-protocol/generated-stove0-observer-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-a1d1a10939"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-79be4a2aa8"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-66c64d45f9"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-observer](../../../evidence/sources/authorities.md#src-dcc0b5485b) — [reference/stove0/packages/observer-support/src/stove0\_observer\_support/schemas.py::observer\_schema\_bundle](../../../../../../reference/stove0/packages/observer-support/src/stove0_observer_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-observer/schemas/ObservationResult`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: af19a3b958f2a964511cf49bd70b49dccc6eae9bf4725ac981bff35e5a46f108 -->

```json
{
  "$defs": {
    "ArtifactSubject": {
      "additionalProperties": false,
      "properties": {
        "bytes": {
          "minimum": 0,
          "title": "Bytes",
          "type": "integer"
        },
        "collection": {
          "$ref": "#/$defs/CollectionRootRef"
        },
        "id": {
          "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "media_type": {
          "anyOf": [
            {
              "maxLength": 255,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Media Type"
        },
        "path": {
          "maxLength": 4096,
          "minLength": 1,
          "title": "Path",
          "type": "string"
        },
        "role": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Role",
          "type": "string"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Sha256",
          "type": "string"
        }
      },
      "required": [
        "id",
        "role",
        "collection",
        "path",
        "bytes",
        "sha256"
      ],
      "title": "ArtifactSubject",
      "type": "object"
    },
    "CollectionId": {
      "allOf": [
        {
          "pattern": "^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[0-1][0-9]{17}|92[0-1][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[0-1][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6][0-9]{0}|9223372036854775807)(?![\\s\\S])",
          "type": "string"
        },
        {
          "not": {
            "const": "0"
          }
        }
      ]
    },
    "CollectionRootRef": {
      "additionalProperties": false,
      "properties": {
        "archive_root_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Archive Root Sha256",
          "type": "string"
        },
        "collection_id": {
          "$ref": "#/$defs/CollectionId"
        },
        "content_identity": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Content Identity",
          "type": "string"
        }
      },
      "required": [
        "collection_id",
        "archive_root_sha256",
        "content_identity"
      ],
      "title": "CollectionRootRef",
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
    "ObservationFailure": {
      "additionalProperties": false,
      "properties": {
        "code": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Code",
          "type": "string"
        },
        "message": {
          "maxLength": 1000,
          "minLength": 1,
          "title": "Message",
          "type": "string"
        },
        "retryable": {
          "title": "Retryable",
          "type": "boolean"
        }
      },
      "required": [
        "code",
        "message",
        "retryable"
      ],
      "title": "ObservationFailure",
      "type": "object"
    },
    "ObservationInapplicable": {
      "additionalProperties": false,
      "properties": {
        "code": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Code",
          "type": "string"
        },
        "message": {
          "maxLength": 1000,
          "minLength": 1,
          "title": "Message",
          "type": "string"
        }
      },
      "required": [
        "code",
        "message"
      ],
      "title": "ObservationInapplicable",
      "type": "object"
    },
    "ObserverImplementation": {
      "additionalProperties": false,
      "properties": {
        "descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Descriptor Sha256",
          "type": "string"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
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
        },
        "version": {
          "maxLength": 120,
          "minLength": 1,
          "title": "Version",
          "type": "string"
        }
      },
      "required": [
        "id",
        "version",
        "source_revision",
        "descriptor_sha256"
      ],
      "title": "ObserverImplementation",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "execution_evidence": {
      "additionalProperties": {
        "$ref": "#/$defs/JsonValue"
      },
      "title": "Execution Evidence",
      "type": "object"
    },
    "facts": {
      "anyOf": [
        {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Facts"
    },
    "facts_schema": {
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
    "facts_sha256": {
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
      "title": "Facts Sha256"
    },
    "failure": {
      "anyOf": [
        {
          "$ref": "#/$defs/ObservationFailure"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "format": {
      "const": "stove0-observation-result/v1",
      "default": "stove0-observation-result/v1",
      "title": "Format",
      "type": "string"
    },
    "inapplicable": {
      "anyOf": [
        {
          "$ref": "#/$defs/ObservationInapplicable"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "observer": {
      "$ref": "#/$defs/ObserverImplementation"
    },
    "observer_contract_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Observer Contract Id",
      "type": "string"
    },
    "observer_contract_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Observer Contract Sha256",
      "type": "string"
    },
    "request_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Request Id",
      "type": "string"
    },
    "result_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Result Sha256",
      "type": "string"
    },
    "state": {
      "enum": [
        "observed",
        "inapplicable",
        "failed",
        "canceled"
      ],
      "title": "State",
      "type": "string"
    },
    "subjects": {
      "items": {
        "$ref": "#/$defs/ArtifactSubject"
      },
      "minItems": 1,
      "title": "Subjects",
      "type": "array"
    }
  },
  "required": [
    "request_id",
    "state",
    "observer",
    "observer_contract_id",
    "observer_contract_sha256",
    "subjects",
    "result_sha256"
  ],
  "title": "ObservationResult",
  "type": "object"
}
```

</details>
