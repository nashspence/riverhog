# generated:stove0-observer: ContentObservationResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:stove0-observer-support:generated-stove0-observer-contentobservationresult:ba5cc796c7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-fb356c1932"></a>

- <a id="s-bcd3425c9a"></a>`type`: `"object"`
- <a id="s-3bd8c0c76a"></a>`additionalProperties`: `false`
- <a id="s-fe68dbfab7"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`
- <a id="s-2327972e7b"></a>`title`: `"ContentObservationResult"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-caf7326745"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-97e1100807)); title="Execution Evidence" |  |
| <a id="s-5f3627eed2"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-97e1100807))); (type="null")]; default=null; title="Facts" |  |
| <a id="s-8b2b8d2633"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-0500ea268f)); (type="null")]; default=null |  |
| <a id="s-331bc41d23"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Facts Sha256" |  |
| <a id="s-0edc45d082"></a>`failure` | no | anyOf=[([ContentObservationFailure](#s-0f116c1648)); (type="null")]; default=null |  |
| <a id="s-545422a18e"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1"; title="Format" |  |
| <a id="s-a96cf47c3a"></a>`inapplicable` | no | anyOf=[([ContentObservationInapplicable](#s-1973573b01)); (type="null")]; default=null |  |
| <a id="s-cad665e97e"></a>`observer` | yes | [ObserverImplementation](#s-c872f7d70c) |  |
| <a id="s-d3b4654d3c"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Observer Contract Id" |  |
| <a id="s-fa42a387e2"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Contract Sha256" |  |
| <a id="s-6a7d9a0986"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Id" |  |
| <a id="s-66fe3da919"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Result Sha256" |  |
| <a id="s-03c2d4f63e"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"]; title="State" |  |
| <a id="s-1ae4facf8c"></a>`subjects` | yes | type="array"; items=([WorkArtifactSubject](#s-f1cf505642)); minItems=1; title="Subjects" |  |

### Definitions

- [CollectionId](#s-d11cc1aac2)
- [CollectionRootIdentityRef](#s-712e0a2afb)
- [ContentObservationFailure](#s-0f116c1648)
- [ContentObservationInapplicable](#s-1973573b01)
- [JsonSchemaValidationProfile](#s-0500ea268f)
- [JsonValue](#s-97e1100807)
- [NonnegativeDecimal](#s-8a8a0425ea)
- [ObserverImplementation](#s-c872f7d70c)
- [WorkArtifactSubject](#s-f1cf505642)

### <a id="s-d11cc1aac2"></a>definition `CollectionId`


#### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-3e85610ab9"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-c24e129734"></a>2 | not=(const="0") |

### <a id="s-712e0a2afb"></a>definition `CollectionRootIdentityRef`

- <a id="s-8340ba76db"></a>`type`: `"object"`
- <a id="s-0540fa18b2"></a>`additionalProperties`: `false`
- <a id="s-d4bb73f5ff"></a>`description`: `"Embedded Stove0 reference to the Riverhog collection-root identity."`
- <a id="s-4325cc4995"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`
- <a id="s-985c9affd0"></a>`title`: `"CollectionRootIdentityRef"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0d22033b44"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Archive Root Sha256" |  |
| <a id="s-d2bfb91ed9"></a>`collection_id` | yes | [CollectionId](#s-d11cc1aac2) |  |
| <a id="s-f68f347eeb"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Content Identity" |  |

### <a id="s-0f116c1648"></a>definition `ContentObservationFailure`

- <a id="s-5f8b229ca6"></a>`type`: `"object"`
- <a id="s-a564d5596d"></a>`additionalProperties`: `false`
- <a id="s-990bec7b51"></a>`required`: `["code","message","retryable"]`
- <a id="s-21c1bce84f"></a>`title`: `"ContentObservationFailure"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-87b9d1f90a"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-aec4462193"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |
| <a id="s-acca989fcc"></a>`retryable` | yes | type="boolean"; title="Retryable" |  |

### <a id="s-1973573b01"></a>definition `ContentObservationInapplicable`

- <a id="s-5c07d3b75b"></a>`type`: `"object"`
- <a id="s-b1ef2c6ea3"></a>`additionalProperties`: `false`
- <a id="s-6f2b7423b0"></a>`required`: `["code","message"]`
- <a id="s-bfb7f59cbd"></a>`title`: `"ContentObservationInapplicable"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2e0ae92ebb"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Code" |  |
| <a id="s-2066dc2858"></a>`message` | yes | type="string"; maxLength=1000; minLength=1; title="Message" |  |

### <a id="s-0500ea268f"></a>definition `JsonSchemaValidationProfile`

- <a id="s-a7d5afc70d"></a>`type`: `"object"`
- <a id="s-49c99c29cf"></a>`additionalProperties`: `false`
- <a id="s-ae812c42f7"></a>`required`: `["id","profile_sha256","schema"]`
- <a id="s-648c047b48"></a>`title`: `"JsonSchemaValidationProfile"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-37a8e2ebdf"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema"; title="Dialect" |  |
| <a id="s-3efcacb6f3"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only"; title="Format Policy" |  |
| <a id="s-a7b9728aac"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-689855e68b"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-b7889e7bb7"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-97e1100807)); title="Schema" |  |

### <a id="s-97e1100807"></a>definition `JsonValue`

- Accepts: any JSON value.

### <a id="s-8a8a0425ea"></a>definition `NonnegativeDecimal`

- <a id="s-41b6325307"></a>`type`: `"string"`
- <a id="s-031abd6667"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

### <a id="s-c872f7d70c"></a>definition `ObserverImplementation`

- <a id="s-4708c8b813"></a>`type`: `"object"`
- <a id="s-a137a39162"></a>`additionalProperties`: `false`
- <a id="s-b6b0c40f2d"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`
- <a id="s-3b4ac587d0"></a>`title`: `"ObserverImplementation"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3e56677dd0"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Descriptor Sha256" |  |
| <a id="s-ecfa6df1ca"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-a079edf05d"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1"; title="Protocol" |  |
| <a id="s-ec530c573d"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1; title="Source Revision" |  |
| <a id="s-578b44f67d"></a>`version` | yes | type="string"; maxLength=120; minLength=1; title="Version" |  |

### <a id="s-f1cf505642"></a>definition `WorkArtifactSubject`

- <a id="s-9a1b66be8a"></a>`type`: `"object"`
- <a id="s-b15e29a833"></a>`additionalProperties`: `false`
- <a id="s-1c3172726f"></a>`description`: `"A collection logical file assigned an ID and role within one Stove0 work."`
- <a id="s-bd9ab433f8"></a>`required`: `["id","role","collection","path","bytes","sha256"]`
- <a id="s-323e58e096"></a>`title`: `"WorkArtifactSubject"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-96059ee9bc"></a>`bytes` | yes | [NonnegativeDecimal](#s-8a8a0425ea); ge=0 |  |
| <a id="s-47754b93d7"></a>`collection` | yes | [CollectionRootIdentityRef](#s-712e0a2afb) |  |
| <a id="s-1895f67053"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-eec3657e82"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null; title="Media Type" |  |
| <a id="s-bbef9c8a11"></a>`path` | yes | type="string"; maxLength=4096; minLength=1; title="Path" |  |
| <a id="s-60ef9e26b8"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |
| <a id="s-ffaa61c462"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-observer-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field execution_evidence](#s-caf7326745) | `cardinality · entries · operational_policy` | shared above |
| <a id="s-d208c8bd93"></a>[field facts · object value](#s-5f3627eed2) | `cardinality · entries · operational_policy` | shared above |
| [field subjects](#s-1ae4facf8c) | `cardinality · items · operational_policy` | shared above |
| [definition JsonSchemaValidationProfile · field schema](#s-b7889e7bb7) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-751d51c356"></a>[field facts_sha256 · string value](#s-331bc41d23) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field observer_contract_sha256](#s-fa42a387e2) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field request_id](#s-6a7d9a0986) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field result_sha256](#s-66fe3da919) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ContentObservationFailure · field message](#s-aec4462193) | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |
| [definition ContentObservationInapplicable · field message](#s-2066dc2858) | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |
| [definition JsonSchemaValidationProfile · field profile_sha256](#s-689855e68b) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObserverImplementation · field descriptor_sha256](#s-3e56677dd0) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObserverImplementation · field source_revision](#s-ec530c573d) | `length · characters · contract_max` | maximum=200; minimum=1; reason="schema-maximum" |
| [definition ObserverImplementation · field version](#s-578b44f67d) | `length · characters · contract_max` | maximum=120; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Related interface records

- [generated:stove0-observer protocol](../process-protocol/generated-stove0-observer-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-af3cd3b255"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-3cb586698f"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-409ba975de"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-observer](../../../evidence/sources/authorities.md#src-dcc0b5485b) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/schemas.py::observer\_schema\_bundle](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-observer/schemas/ContentObservationResult`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d0a8163fbd6a3bc9dbeec9197227d29fe66932029b0166b970855483cd7dfa7a -->

```json
{
  "$defs": {
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
    "CollectionRootIdentityRef": {
      "additionalProperties": false,
      "description": "Embedded Stove0 reference to the Riverhog collection-root identity.",
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
      "title": "CollectionRootIdentityRef",
      "type": "object"
    },
    "ContentObservationFailure": {
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
      "title": "ContentObservationFailure",
      "type": "object"
    },
    "ContentObservationInapplicable": {
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
      "title": "ContentObservationInapplicable",
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
    "NonnegativeDecimal": {
      "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
      "type": "string"
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
    },
    "WorkArtifactSubject": {
      "additionalProperties": false,
      "description": "A collection logical file assigned an ID and role within one Stove0 work.",
      "properties": {
        "bytes": {
          "$ref": "#/$defs/NonnegativeDecimal",
          "ge": 0
        },
        "collection": {
          "$ref": "#/$defs/CollectionRootIdentityRef"
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
      "title": "WorkArtifactSubject",
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
          "$ref": "#/$defs/ContentObservationFailure"
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
          "$ref": "#/$defs/ContentObservationInapplicable"
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
        "$ref": "#/$defs/WorkArtifactSubject"
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
  "title": "ContentObservationResult",
  "type": "object"
}
```

</details>
