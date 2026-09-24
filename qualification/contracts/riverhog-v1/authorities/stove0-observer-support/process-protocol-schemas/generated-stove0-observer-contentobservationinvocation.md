# generated:stove0-observer: ContentObservationInvocation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:stove0-observer-support:generated-stove0-observer-contentobservat-9e12af9600:6bdee3e4e6 -->

Fence-bound invocation authority excluded from semantic request identity.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-5b291b7856"></a>

- <a id="s-bce7b87275"></a>`type`: `"object"`
- <a id="s-49bb816ef6"></a>`additionalProperties`: `false`
- <a id="s-b3a0dfe03e"></a>`description`: `"Fence-bound invocation authority excluded from semantic request identity."`
- <a id="s-529f0d9df4"></a>`required`: `["request","claim_id","fence","runtime"]`
- <a id="s-77f9cfb3e3"></a>`title`: `"ContentObservationInvocation"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bc1454bc8d"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1; title="Claim Id" |  |
| <a id="s-c56fea0837"></a>`fence` | yes | type="integer"; minimum=1; title="Fence" |  |
| <a id="s-1356ccbbf1"></a>`request` | yes | [ContentObservationRequest](#s-7826415ecb) |  |
| <a id="s-b2ae70ec2f"></a>`runtime` | yes | [ObserverRuntimeAuthority](#s-638f987df2) |  |

### Definitions

- [CollectionId](#s-c4dc4fb8cc)
- [CollectionRootIdentityRef](#s-d1cdeea832)
- [ContentObservationRequest](#s-7826415ecb)
- [DeclaredWorkspaceProtection](#s-9690597347)
- [JsonValue](#s-3cd351bac8)
- [ObserverRuntimeAuthority](#s-638f987df2)
- [WorkArtifactSubject](#s-841f4a0d30)

### <a id="s-c4dc4fb8cc"></a>definition `CollectionId`


#### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-e0d857f8b3"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-e5d5ffe2b2"></a>2 | not=(const="0") |

### <a id="s-d1cdeea832"></a>definition `CollectionRootIdentityRef`

- <a id="s-c25da3619f"></a>`type`: `"object"`
- <a id="s-b6e38073cc"></a>`additionalProperties`: `false`
- <a id="s-8b41afe093"></a>`description`: `"Embedded Stove0 reference to the Riverhog collection-root identity."`
- <a id="s-f38ab716e2"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`
- <a id="s-9b51a42306"></a>`title`: `"CollectionRootIdentityRef"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-146ba395be"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Archive Root Sha256" |  |
| <a id="s-2ef9f4a452"></a>`collection_id` | yes | [CollectionId](#s-c4dc4fb8cc) |  |
| <a id="s-df26c030e1"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Content Identity" |  |

### <a id="s-7826415ecb"></a>definition `ContentObservationRequest`

- <a id="s-b2a518e603"></a>`type`: `"object"`
- <a id="s-efbad13417"></a>`additionalProperties`: `false`
- <a id="s-9258a26c52"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`
- <a id="s-4424c1c8fb"></a>`title`: `"ContentObservationRequest"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-64a94fe798"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1"; title="Format" |  |
| <a id="s-4923f7df82"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576; title="Maximum Result Bytes" |  |
| <a id="s-d6d3259e66"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Observer Contract Id" |  |
| <a id="s-36f5b490ae"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Contract Sha256" |  |
| <a id="s-877457dde6"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Observer Descriptor Sha256" |  |
| <a id="s-f22a2b6d9f"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$"; title="Observer Registration Id" |  |
| <a id="s-d55800e706"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-3cd351bac8)); title="Options" |  |
| <a id="s-5252f23a45"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Id" |  |
| <a id="s-ca5710baff"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only"; title="Retrieval Policy" |  |
| <a id="s-37364f1817"></a>`subjects` | yes | type="array"; items=([WorkArtifactSubject](#s-841f4a0d30)); minItems=1; title="Subjects" |  |
| <a id="s-a28ac91763"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300; title="Timeout Seconds" |  |
| <a id="s-a352d45bc8"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Id" |  |

### <a id="s-9690597347"></a>definition `DeclaredWorkspaceProtection`

- <a id="s-892629d5f5"></a>`type`: `"string"`
- <a id="s-accb55c1b6"></a>`enum`: `["encrypted-at-rest","memory-backed"]`
- <a id="s-61c55f147e"></a>`description`: `"Deployment declaration for plaintext workspace storage. Memory-backed storage requires no unencrypted swap. The runtime does not verify the mount or swap policy."`

### <a id="s-3cd351bac8"></a>definition `JsonValue`

- Accepts: any JSON value.

### <a id="s-638f987df2"></a>definition `ObserverRuntimeAuthority`

- <a id="s-1cda3fde4b"></a>`type`: `"object"`
- <a id="s-52bda403e7"></a>`additionalProperties`: `false`
- <a id="s-a96804d5d5"></a>`description`: `"Secret-bearing invocation material excluded from durable request identity."`
- <a id="s-e30df2f13d"></a>`required`: `["riverhog_base_url","capability_token","declared_workspace_protection"]`
- <a id="s-75373e9e2e"></a>`title`: `"ObserverRuntimeAuthority"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6d6f79f139"></a>`allow_insecure_http` | no | type="boolean"; default=false; title="Allow Insecure Http" |  |
| <a id="s-ed70ef4bc7"></a>`capability_token` | yes | type="string"; maxLength=4096; minLength=1; title="Capability Token" |  |
| <a id="s-8e51fcb5e2"></a>`declared_workspace_protection` | yes | [DeclaredWorkspaceProtection](#s-9690597347) |  |
| <a id="s-df16966ff6"></a>`riverhog_base_url` | yes | type="string"; maxLength=2048; minLength=1; title="Riverhog Base Url" |  |
| <a id="s-ff2b6896ae"></a>`transport` | no | type="string"; const="riverhog-capability/v1"; default="riverhog-capability/v1"; title="Transport" |  |

### <a id="s-841f4a0d30"></a>definition `WorkArtifactSubject`

- <a id="s-7a774a7813"></a>`type`: `"object"`
- <a id="s-45c785cd39"></a>`additionalProperties`: `false`
- <a id="s-f029b9462a"></a>`description`: `"A collection logical file assigned an ID and role within one Stove0 work."`
- <a id="s-6ca80e5ab2"></a>`required`: `["id","role","collection","path","bytes","sha256"]`
- <a id="s-527419699b"></a>`title`: `"WorkArtifactSubject"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c6d6197898"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-a26dfc3c74"></a>`collection` | yes | [CollectionRootIdentityRef](#s-d1cdeea832) |  |
| <a id="s-62caa094ad"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-732445ae0e"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null; title="Media Type" |  |
| <a id="s-b05cd9a31b"></a>`path` | yes | type="string"; maxLength=4096; minLength=1; title="Path" |  |
| <a id="s-f0fcd680fb"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |
| <a id="s-2f264373cd"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-observer-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition ContentObservationRequest · field options](#s-d55800e706) | `cardinality · entries · operational_policy` | shared above |
| [definition ContentObservationRequest · field subjects](#s-37364f1817) | `cardinality · items · operational_policy` | shared above |
| [definition WorkArtifactSubject · field bytes](#s-c6d6197898) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field claim_id](#s-bc1454bc8d) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [definition CollectionRootIdentityRef · field archive_root_sha256](#s-146ba395be) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition CollectionRootIdentityRef · field content_identity](#s-df26c030e1) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ContentObservationRequest · field maximum_result_bytes](#s-4923f7df82) | `value · schema-value · contract_max` | maximum=67108864; minimum=1; reason="schema-maximum" |
| [definition ContentObservationRequest · field observer_contract_sha256](#s-36f5b490ae) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ContentObservationRequest · field observer_descriptor_sha256](#s-877457dde6) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ContentObservationRequest · field request_id](#s-5252f23a45) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ContentObservationRequest · field timeout_seconds](#s-a28ac91763) | `value · schema-value · contract_max` | maximum=86400; minimum=1; reason="schema-maximum" |
| [definition ContentObservationRequest · field work_id](#s-a352d45bc8) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObserverRuntimeAuthority · field capability_token](#s-ed70ef4bc7) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [definition ObserverRuntimeAuthority · field riverhog_base_url](#s-df16966ff6) | `length · characters · contract_max` | maximum=2048; minimum=1; reason="schema-maximum" |
| <a id="s-0bef710e11"></a>[definition WorkArtifactSubject · field media_type · string value](#s-732445ae0e) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [definition WorkArtifactSubject · field path](#s-b05cd9a31b) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [definition WorkArtifactSubject · field sha256](#s-2f264373cd) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Related interface records

- [generated:stove0-observer protocol](../process-protocol/generated-stove0-observer-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-02a5ab9de6"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-8c249b5664"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-34803a826d"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-observer](../../../evidence/sources/authorities.md#src-dcc0b5485b) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/schemas.py::observer\_schema\_bundle](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-observer/schemas/ContentObservationInvocation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ffe68a326906c84c2aafd4b2b56230e1f1a68536a7d14e3bc7c1cc29c8568161 -->

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
    "ContentObservationRequest": {
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
            "$ref": "#/$defs/WorkArtifactSubject"
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
      "title": "ContentObservationRequest",
      "type": "object"
    },
    "DeclaredWorkspaceProtection": {
      "description": "Deployment declaration for plaintext workspace storage. Memory-backed storage requires no unencrypted swap. The runtime does not verify the mount or swap policy.",
      "enum": [
        "encrypted-at-rest",
        "memory-backed"
      ],
      "type": "string"
    },
    "JsonValue": {},
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
        "declared_workspace_protection": {
          "$ref": "#/$defs/DeclaredWorkspaceProtection"
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
        }
      },
      "required": [
        "riverhog_base_url",
        "capability_token",
        "declared_workspace_protection"
      ],
      "title": "ObserverRuntimeAuthority",
      "type": "object"
    },
    "WorkArtifactSubject": {
      "additionalProperties": false,
      "description": "A collection logical file assigned an ID and role within one Stove0 work.",
      "properties": {
        "bytes": {
          "minimum": 0,
          "title": "Bytes",
          "type": "integer"
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
      "$ref": "#/$defs/ContentObservationRequest"
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
  "title": "ContentObservationInvocation",
  "type": "object"
}
```

</details>
