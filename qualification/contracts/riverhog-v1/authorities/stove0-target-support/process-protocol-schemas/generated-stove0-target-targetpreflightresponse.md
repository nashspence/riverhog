# generated:stove0-target: TargetPreflightResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:stove0-target-support:generated-stove0-target-targetpreflightresponse:ad83339a9a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-98bf5efc84"></a>

- <a id="s-8265320694"></a>`type`: `"object"`
- <a id="s-cea58fca35"></a>`additionalProperties`: `false`
- <a id="s-5d213f0404"></a>`required`: `["target","plan"]`
- <a id="s-160f48c14c"></a>`title`: `"TargetPreflightResponse"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b2eadc07e6"></a>`plan` | yes | discriminator={"mapping":{"stove0-effect-target/v1":"#/$defs/EffectPlan","stove0-transform-target/v1":"#/$defs/TransformPlan"},"propertyName":"protocol"}; oneOf=[([TransformPlan](#s-2398dee3b8)); ([EffectPlan](#s-11bebc4dd2))]; title="Plan" |  |
| <a id="s-e6c89ac6df"></a>`target` | yes | [TargetContract](#s-d29225bb03) |  |

### Definitions

- [ArtifactSelectionRef](#s-6e82db4fba)
- [EffectPlan](#s-11bebc4dd2)
- [JsonSchemaValidationProfile](#s-112520cc69)
- [JsonValue](#s-885012f602)
- [TargetContract](#s-d29225bb03)
- [TargetInputAuthority](#s-18093afb67)
- [TargetInputRoleCount](#s-026358c5f8)
- [TargetOperationSupport](#s-40a67f974a)
- [TransformPlan](#s-2398dee3b8)

### <a id="s-6e82db4fba"></a>definition `ArtifactSelectionRef`

- <a id="s-2645a3478a"></a>`type`: `"object"`
- <a id="s-013638f8f4"></a>`additionalProperties`: `false`
- <a id="s-cb0df0e97b"></a>`description`: `"Closed reference to a separately retained selection document."`
- <a id="s-d68b08c86c"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`
- <a id="s-7cd5cc9c01"></a>`title`: `"ArtifactSelectionRef"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-91c1a2048a"></a>`artifact_count` | yes | type="integer"; minimum=1; title="Artifact Count" |  |
| <a id="s-eed1f0328c"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Selection Sha256" |  |
| <a id="s-a79f343304"></a>`total_bytes` | yes | type="integer"; minimum=0; title="Total Bytes" |  |

### <a id="s-11bebc4dd2"></a>definition `EffectPlan`

- <a id="s-829e9f8a98"></a>`type`: `"object"`
- <a id="s-1c19661537"></a>`additionalProperties`: `false`
- <a id="s-817f7bafb0"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_contract_sha256","plan_sha256"]`
- <a id="s-30ad2e76a4"></a>`title`: `"EffectPlan"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d368acee72"></a>`inputs` | yes | [TargetInputAuthority](#s-18093afb67) |  |
| <a id="s-51c58b479a"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-885012f602)); title="Intent" |  |
| <a id="s-91627aa6cc"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$"); title="Observation Result Sha256S" |  |
| <a id="s-55dacd3c6d"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-6eccc15ae3"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Operation Id" |  |
| <a id="s-b1431c0cfd"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-a9b7569a60"></a>`protocol` | no | type="string"; const="stove0-effect-target/v1"; default="stove0-effect-target/v1"; title="Protocol" |  |
| <a id="s-ae5cad52fa"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Contract Sha256" |  |
| <a id="s-1728972dbd"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Target Implementation Id" |  |
| <a id="s-253ffbfb8a"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-885012f602)); title="Target Options" |  |

### <a id="s-112520cc69"></a>definition `JsonSchemaValidationProfile`

- <a id="s-4cbd153f09"></a>`type`: `"object"`
- <a id="s-a491afdd5c"></a>`additionalProperties`: `false`
- <a id="s-dd302ab6b7"></a>`required`: `["id","profile_sha256","schema"]`
- <a id="s-c579815d75"></a>`title`: `"JsonSchemaValidationProfile"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ccf698153d"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema"; title="Dialect" |  |
| <a id="s-e85cc97157"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only"; title="Format Policy" |  |
| <a id="s-f2cb0087e5"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-e4ffc1a626"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-f9808eec8c"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-885012f602)); title="Schema" |  |

### <a id="s-885012f602"></a>definition `JsonValue`

- Accepts: any JSON value.

### <a id="s-d29225bb03"></a>definition `TargetContract`

- <a id="s-6c5836d193"></a>`type`: `"object"`
- <a id="s-20c201c3ae"></a>`additionalProperties`: `false`
- <a id="s-e5ac3e1b54"></a>`required`: `["implementation_id","implementation_version","source_revision","image_digest","operations","contract_sha256"]`
- <a id="s-829d904ed1"></a>`title`: `"TargetContract"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-791145bbfa"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Contract Sha256" |  |
| <a id="s-23a84ce5f2"></a>`image_digest` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Image Digest" |  |
| <a id="s-a383521d81"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Implementation Id" |  |
| <a id="s-004149fbb9"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1; title="Implementation Version" |  |
| <a id="s-58a80a3bdd"></a>`operations` | yes | type="array"; items=([TargetOperationSupport](#s-40a67f974a)); minItems=1; title="Operations" |  |
| <a id="s-8a396d9a23"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1"; title="Protocol" |  |
| <a id="s-37e5ac4a4b"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1; title="Source Revision" |  |
| <a id="s-4f552cbb16"></a>`transport` | no | type="string"; const="riverhog-capability/v1"; default="riverhog-capability/v1"; title="Transport" |  |

### <a id="s-18093afb67"></a>definition `TargetInputAuthority`

- <a id="s-868127a5b8"></a>`type`: `"object"`
- <a id="s-55d214b89b"></a>`additionalProperties`: `false`
- <a id="s-97ee97dfdc"></a>`description`: `"Small exact input authority retained by Stove0 and traversed in bounded pages."`
- <a id="s-ec266afcda"></a>`required`: `["selection","roles"]`
- <a id="s-92bd5ff2d3"></a>`title`: `"TargetInputAuthority"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2796eeb1cd"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](#s-026358c5f8)); minItems=1; title="Roles" |  |
| <a id="s-bd3262dcaf"></a>`selection` | yes | [ArtifactSelectionRef](#s-6e82db4fba) |  |

### <a id="s-026358c5f8"></a>definition `TargetInputRoleCount`

- <a id="s-41a482a81b"></a>`type`: `"object"`
- <a id="s-9013d6066f"></a>`additionalProperties`: `false`
- <a id="s-7be7b288a7"></a>`required`: `["role","count"]`
- <a id="s-778ce1e3d9"></a>`title`: `"TargetInputRoleCount"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e212a71813"></a>`count` | yes | type="integer"; minimum=1; title="Count" |  |
| <a id="s-60e41e7c5b"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |

### <a id="s-40a67f974a"></a>definition `TargetOperationSupport`

- <a id="s-9acbafb7fb"></a>`type`: `"object"`
- <a id="s-b1c59e7504"></a>`additionalProperties`: `false`
- <a id="s-5d4f9be278"></a>`required`: `["operation_id","operation_contract_sha256","options_schema"]`
- <a id="s-9d64fc1bb7"></a>`title`: `"TargetOperationSupport"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fb06e03c39"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-6735f2b9dc"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Operation Id" |  |
| <a id="s-e2bceafed9"></a>`options_schema` | yes | [JsonSchemaValidationProfile](#s-112520cc69) |  |
| <a id="s-1bfa3e093c"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection"; title="Result Kind" |  |

### <a id="s-2398dee3b8"></a>definition `TransformPlan`

- <a id="s-c92bc46869"></a>`type`: `"object"`
- <a id="s-925d0a347d"></a>`additionalProperties`: `false`
- <a id="s-aa1e52a977"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_contract_sha256","plan_sha256"]`
- <a id="s-28e48aa423"></a>`title`: `"TransformPlan"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0a787fb4eb"></a>`inputs` | yes | [TargetInputAuthority](#s-18093afb67) |  |
| <a id="s-237cb127e7"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-885012f602)); title="Intent" |  |
| <a id="s-4bb56be6c8"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$"); title="Observation Result Sha256S" |  |
| <a id="s-92eab5005b"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-2c38e1fa91"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Operation Id" |  |
| <a id="s-bf99e64d9a"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-509f9c011c"></a>`protocol` | no | type="string"; const="stove0-transform-target/v1"; default="stove0-transform-target/v1"; title="Protocol" |  |
| <a id="s-128c56a7e7"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Contract Sha256" |  |
| <a id="s-0a2c2cc978"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Target Implementation Id" |  |
| <a id="s-3595c033b4"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-885012f602)); title="Target Options" |  |

## Maintained corroboration

### Related interface records

- [generated:stove0-target protocol](../process-protocol/generated-stove0-target-protocol.md)

## Governing policies

- <a id="pa-3a06de272d"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-target](../../../evidence/sources/authorities.md#src-2c42f9d39a) — [reference/stove0/packages/target-support/src/stove0\_target\_support/schemas.py::target\_schema\_bundle](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/schemas/TargetPreflightResponse`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7b609fe2c99ef6758609d7c9071820f946498e89b455143b9edcd47df8fa08a5 -->

```json
{
  "$defs": {
    "ArtifactSelectionRef": {
      "additionalProperties": false,
      "description": "Closed reference to a separately retained selection document.",
      "properties": {
        "artifact_count": {
          "minimum": 1,
          "title": "Artifact Count",
          "type": "integer"
        },
        "selection_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Selection Sha256",
          "type": "string"
        },
        "total_bytes": {
          "minimum": 0,
          "title": "Total Bytes",
          "type": "integer"
        }
      },
      "required": [
        "selection_sha256",
        "artifact_count",
        "total_bytes"
      ],
      "title": "ArtifactSelectionRef",
      "type": "object"
    },
    "EffectPlan": {
      "additionalProperties": false,
      "properties": {
        "inputs": {
          "$ref": "#/$defs/TargetInputAuthority"
        },
        "intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Intent",
          "type": "object"
        },
        "observation_result_sha256s": {
          "default": [],
          "items": {
            "pattern": "^[0-9a-f]{64}$",
            "type": "string"
          },
          "title": "Observation Result Sha256S",
          "type": "array"
        },
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
        "plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Plan Sha256",
          "type": "string"
        },
        "protocol": {
          "const": "stove0-effect-target/v1",
          "default": "stove0-effect-target/v1",
          "title": "Protocol",
          "type": "string"
        },
        "target_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Target Contract Sha256",
          "type": "string"
        },
        "target_implementation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Target Implementation Id",
          "type": "string"
        },
        "target_options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Target Options",
          "type": "object"
        }
      },
      "required": [
        "operation_id",
        "operation_contract_sha256",
        "inputs",
        "intent",
        "target_implementation_id",
        "target_contract_sha256",
        "plan_sha256"
      ],
      "title": "EffectPlan",
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
    "TargetContract": {
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
    },
    "TargetInputAuthority": {
      "additionalProperties": false,
      "description": "Small exact input authority retained by Stove0 and traversed in bounded pages.",
      "properties": {
        "roles": {
          "items": {
            "$ref": "#/$defs/TargetInputRoleCount"
          },
          "minItems": 1,
          "title": "Roles",
          "type": "array"
        },
        "selection": {
          "$ref": "#/$defs/ArtifactSelectionRef"
        }
      },
      "required": [
        "selection",
        "roles"
      ],
      "title": "TargetInputAuthority",
      "type": "object"
    },
    "TargetInputRoleCount": {
      "additionalProperties": false,
      "properties": {
        "count": {
          "minimum": 1,
          "title": "Count",
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
        "count"
      ],
      "title": "TargetInputRoleCount",
      "type": "object"
    },
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
    },
    "TransformPlan": {
      "additionalProperties": false,
      "properties": {
        "inputs": {
          "$ref": "#/$defs/TargetInputAuthority"
        },
        "intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Intent",
          "type": "object"
        },
        "observation_result_sha256s": {
          "default": [],
          "items": {
            "pattern": "^[0-9a-f]{64}$",
            "type": "string"
          },
          "title": "Observation Result Sha256S",
          "type": "array"
        },
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
        "plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Plan Sha256",
          "type": "string"
        },
        "protocol": {
          "const": "stove0-transform-target/v1",
          "default": "stove0-transform-target/v1",
          "title": "Protocol",
          "type": "string"
        },
        "target_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Target Contract Sha256",
          "type": "string"
        },
        "target_implementation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Target Implementation Id",
          "type": "string"
        },
        "target_options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Target Options",
          "type": "object"
        }
      },
      "required": [
        "operation_id",
        "operation_contract_sha256",
        "inputs",
        "intent",
        "target_implementation_id",
        "target_contract_sha256",
        "plan_sha256"
      ],
      "title": "TransformPlan",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "plan": {
      "discriminator": {
        "mapping": {
          "stove0-effect-target/v1": "#/$defs/EffectPlan",
          "stove0-transform-target/v1": "#/$defs/TransformPlan"
        },
        "propertyName": "protocol"
      },
      "oneOf": [
        {
          "$ref": "#/$defs/TransformPlan"
        },
        {
          "$ref": "#/$defs/EffectPlan"
        }
      ],
      "title": "Plan"
    },
    "target": {
      "$ref": "#/$defs/TargetContract"
    }
  },
  "required": [
    "target",
    "plan"
  ],
  "title": "TargetPreflightResponse",
  "type": "object"
}
```

</details>
