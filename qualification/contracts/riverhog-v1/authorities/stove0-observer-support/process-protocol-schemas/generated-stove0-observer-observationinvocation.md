# generated:stove0-observer: ObservationInvocation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:stove0-observer-support:generated-stove0-observer-observationinvocation:88650173d8 -->

Fence-bound invocation authority excluded from semantic request identity.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-5cc4b3adc3"></a>

- <a id="s-3a530566b1"></a>`type`: `"object"`
- <a id="s-28af6addea"></a>`additionalProperties`: `false`
- <a id="s-10ba72f37a"></a>`description`: `"Fence-bound invocation authority excluded from semantic request identity."`
- <a id="s-afde2d0373"></a>`required`: `["request","claim_id","fence","runtime"]`
- <a id="s-d5a41e9392"></a>`title`: `"ObservationInvocation"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-072f4b544c"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1; title="Claim Id" |  |
| <a id="s-d483a59ad8"></a>`fence` | yes | type="integer"; minimum=1; title="Fence" |  |
| <a id="s-5aa86a4371"></a>`request` | yes | [ObservationRequest](#s-47065fc11b) |  |
| <a id="s-0109a56940"></a>`runtime` | yes | [ObserverRuntimeAuthority](#s-f89907f0ff) |  |

### Definitions

- [ArtifactSubject](#s-6ecabdecfa)
- [CollectionId](#s-90dcdf64ae)
- [CollectionRootRef](#s-ad3843b90a)
- [JsonValue](#s-95de9a9e67)
- [ObservationRequest](#s-47065fc11b)
- [ObserverRuntimeAuthority](#s-f89907f0ff)

### <a id="s-6ecabdecfa"></a>definition `ArtifactSubject`

- <a id="s-ae0fbabbb3"></a>`type`: `"object"`
- <a id="s-1f0cf921a1"></a>`additionalProperties`: `false`
- <a id="s-4670e41738"></a>`required`: `["id","role","collection","path","bytes","sha256"]`
- <a id="s-1e77492952"></a>`title`: `"ArtifactSubject"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2a086b4455"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-711af1ff17"></a>`collection` | yes | [CollectionRootRef](#s-ad3843b90a) |  |
| <a id="s-d7dca27ecc"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-23e67c27bf"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null; title="Media Type" |  |
| <a id="s-19b80a5714"></a>`path` | yes | type="string"; maxLength=4096; minLength=1; title="Path" |  |
| <a id="s-d8821ee9e6"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |
| <a id="s-bc561903c6"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### <a id="s-90dcdf64ae"></a>definition `CollectionId`


#### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-362d08619f"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-25c9220101"></a>2 | not=(const="0") |

### <a id="s-ad3843b90a"></a>definition `CollectionRootRef`

- <a id="s-55c1b27fec"></a>`type`: `"object"`
- <a id="s-9ec54c1bfd"></a>`additionalProperties`: `false`
- <a id="s-a72788df5a"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`
- <a id="s-b7901168b2"></a>`title`: `"CollectionRootRef"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7de457ad21"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Archive Root Sha256" |  |
| <a id="s-890e5f35a9"></a>`collection_id` | yes | [CollectionId](#s-90dcdf64ae) |  |
| <a id="s-13ae4d5efc"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Content Identity" |  |

### <a id="s-95de9a9e67"></a>definition `JsonValue`

- Accepts: any JSON value.

### <a id="s-47065fc11b"></a>definition `ObservationRequest`

- <a id="s-1ca52baefd"></a>`type`: `"object"`
- <a id="s-c5b97bba42"></a>`additionalProperties`: `false`
- <a id="s-a635e43b4d"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`
- <a id="s-71d9813327"></a>`title`: `"ObservationRequest"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-00e4c43523"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1"; title="Format" |  |
| <a id="s-938eb9b92c"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576; title="Maximum Result Bytes" |  |
| <a id="s-3d4c562d71"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Observer Contract Id" |  |
| <a id="s-01059693d6"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Contract Sha256" |  |
| <a id="s-df85f4c9be"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Descriptor Sha256" |  |
| <a id="s-1fc32fa596"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$"; title="Observer Registration Id" |  |
| <a id="s-df3b33cb99"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-95de9a9e67)); title="Options" |  |
| <a id="s-25438bc744"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Id" |  |
| <a id="s-6f9e59c19c"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only"; title="Retrieval Policy" |  |
| <a id="s-433a287632"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-6ecabdecfa)); minItems=1; title="Subjects" |  |
| <a id="s-a884a49972"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300; title="Timeout Seconds" |  |
| <a id="s-e81ed70f43"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Id" |  |

### <a id="s-f89907f0ff"></a>definition `ObserverRuntimeAuthority`

- <a id="s-23bb705710"></a>`type`: `"object"`
- <a id="s-45ce1d1f88"></a>`additionalProperties`: `false`
- <a id="s-9ad56379bc"></a>`description`: `"Secret-bearing invocation material excluded from durable request identity."`
- <a id="s-bc2d59cca4"></a>`required`: `["riverhog_base_url","capability_token","workspace_assurance"]`
- <a id="s-aafcde42a8"></a>`title`: `"ObserverRuntimeAuthority"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3208b3767d"></a>`allow_insecure_http` | no | type="boolean"; default=false; title="Allow Insecure Http" |  |
| <a id="s-a17c050adb"></a>`capability_token` | yes | type="string"; maxLength=4096; minLength=1; title="Capability Token" |  |
| <a id="s-aea2c68d3e"></a>`riverhog_base_url` | yes | type="string"; maxLength=2048; minLength=1; title="Riverhog Base Url" |  |
| <a id="s-d285880be0"></a>`transport` | no | type="string"; const="riverhog-capability/v1"; default="riverhog-capability/v1"; title="Transport" |  |
| <a id="s-5a67beef1e"></a>`workspace_assurance` | yes | type="string"; enum=["encrypted","ephemeral"]; title="Workspace Assurance" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-observer-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition ArtifactSubject · field bytes](#s-2a086b4455) | `value · schema-value · operational_policy` | shared above |
| [definition ObservationRequest · field options](#s-df3b33cb99) | `cardinality · entries · operational_policy` | shared above |
| [definition ObservationRequest · field subjects](#s-433a287632) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field claim_id](#s-072f4b544c) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| <a id="s-775d0ab059"></a>[definition ArtifactSubject · field media_type · string value](#s-23e67c27bf) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [definition ArtifactSubject · field path](#s-19b80a5714) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [definition ArtifactSubject · field sha256](#s-bc561903c6) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition CollectionRootRef · field archive_root_sha256](#s-7de457ad21) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition CollectionRootRef · field content_identity](#s-13ae4d5efc) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObservationRequest · field maximum_result_bytes](#s-938eb9b92c) | `value · schema-value · contract_max` | maximum=67108864; minimum=1; reason="schema-maximum" |
| [definition ObservationRequest · field observer_contract_sha256](#s-01059693d6) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObservationRequest · field observer_descriptor_sha256](#s-df85f4c9be) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObservationRequest · field request_id](#s-25438bc744) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObservationRequest · field timeout_seconds](#s-a884a49972) | `value · schema-value · contract_max` | maximum=86400; minimum=1; reason="schema-maximum" |
| [definition ObservationRequest · field work_id](#s-e81ed70f43) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObserverRuntimeAuthority · field capability_token](#s-a17c050adb) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [definition ObserverRuntimeAuthority · field riverhog_base_url](#s-aea2c68d3e) | `length · characters · contract_max` | maximum=2048; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Related interface records

- [generated:stove0-observer protocol](../process-protocol/generated-stove0-observer-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-d4877795f9"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-b88d960466"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-7cda2da729"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-observer](../../../evidence/sources/authorities.md#src-dcc0b5485b) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/schemas.py::observer\_schema\_bundle](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-observer/schemas/ObservationInvocation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dcc85384eb9f0358fe7190843535b2acb12d43b9a285e0a02be4c86d01251dda -->

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
    "JsonValue": {},
    "ObservationRequest": {
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "stove0-observation-request/v1",
          "default": "stove0-observation-request/v1",
          "title": "Format",
          "type": "string"
        },
        "maximum_result_bytes": {
          "default": 1048576,
          "maximum": 67108864,
          "minimum": 1,
          "title": "Maximum Result Bytes",
          "type": "integer"
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
        "observer_descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Observer Descriptor Sha256",
          "type": "string"
        },
        "observer_registration_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$",
          "title": "Observer Registration Id",
          "type": "string"
        },
        "options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Options",
          "type": "object"
        },
        "request_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Request Id",
          "type": "string"
        },
        "retrieval_policy": {
          "default": "available-only",
          "enum": [
            "available-only",
            "allow"
          ],
          "title": "Retrieval Policy",
          "type": "string"
        },
        "subjects": {
          "items": {
            "$ref": "#/$defs/ArtifactSubject"
          },
          "minItems": 1,
          "title": "Subjects",
          "type": "array"
        },
        "timeout_seconds": {
          "default": 300,
          "maximum": 86400,
          "minimum": 1,
          "title": "Timeout Seconds",
          "type": "integer"
        },
        "work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Work Id",
          "type": "string"
        }
      },
      "required": [
        "work_id",
        "observer_registration_id",
        "observer_descriptor_sha256",
        "observer_contract_id",
        "observer_contract_sha256",
        "subjects",
        "request_id"
      ],
      "title": "ObservationRequest",
      "type": "object"
    },
    "ObserverRuntimeAuthority": {
      "additionalProperties": false,
      "description": "Secret-bearing invocation material excluded from durable request identity.",
      "properties": {
        "allow_insecure_http": {
          "default": false,
          "title": "Allow Insecure Http",
          "type": "boolean"
        },
        "capability_token": {
          "maxLength": 4096,
          "minLength": 1,
          "title": "Capability Token",
          "type": "string"
        },
        "riverhog_base_url": {
          "maxLength": 2048,
          "minLength": 1,
          "title": "Riverhog Base Url",
          "type": "string"
        },
        "transport": {
          "const": "riverhog-capability/v1",
          "default": "riverhog-capability/v1",
          "title": "Transport",
          "type": "string"
        },
        "workspace_assurance": {
          "enum": [
            "encrypted",
            "ephemeral"
          ],
          "title": "Workspace Assurance",
          "type": "string"
        }
      },
      "required": [
        "riverhog_base_url",
        "capability_token",
        "workspace_assurance"
      ],
      "title": "ObserverRuntimeAuthority",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "description": "Fence-bound invocation authority excluded from semantic request identity.",
  "properties": {
    "claim_id": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Claim Id",
      "type": "string"
    },
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    },
    "request": {
      "$ref": "#/$defs/ObservationRequest"
    },
    "runtime": {
      "$ref": "#/$defs/ObserverRuntimeAuthority"
    }
  },
  "required": [
    "request",
    "claim_id",
    "fence",
    "runtime"
  ],
  "title": "ObservationInvocation",
  "type": "object"
}
```

</details>
