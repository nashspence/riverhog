# stove0_target_protocol.TargetPreflightResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetpreflightresponse:97c1b950c1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e11161fe1a"></a>
- <a id="s-dbc23250dc"></a>`distribution`: `stove0-target-protocol`
- <a id="s-a3e9db6ac2"></a>`module`: `stove0_target_protocol`
- <a id="s-edd654d3be"></a>`name`: `TargetPreflightResponse`
- <a id="s-1ef7f4c649"></a>`unit`: `export`

### Declared structure

- <a id="s-53706f04df"></a>`kind`: `"class"`
- <a id="s-9363613723"></a>`signature`: `"'(*, target: stove0_target_protocol.protocol.TargetContract, plan: stove0_target_protocol.protocol.TransformPlan \| stove0_target_protocol.protocol.EffectPlan) -> None'"`

#### Validated model schema

<a id="s-4758c4be59"></a>
- <a id="s-3030869a4d"></a>`title`: TargetPreflightResponse
- <a id="s-64a96bafdd"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-adc278d8ef"></a>`plan` | yes | oneOf=#/$defs/TransformPlan \| #/$defs/EffectPlan; additional keys=`discriminator` |  |
| <a id="s-3e49a2693d"></a>`target` | yes | #/$defs/TargetContract |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-e634dd1750"></a>`ArtifactSelectionRef` | type="object"; fields=`artifact_count`, `selection_sha256`, `total_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-76c6471c9f"></a>`EffectPlan` | type="object"; fields=`inputs`, `intent`, `observation_result_sha256s`, `operation_contract_sha256`, `operation_id`, `plan_sha256`, `protocol`, `target_contract_sha256`, `target_implementation_id`, `target_options`; additional keys=`additionalProperties`, `required` |
| <a id="s-3c2c8453b1"></a>`JsonSchemaDocument` | type="object"; fields=`dialect`, `format_policy`, `id`, `schema`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-9f4f47f35d"></a>`JsonValue` | empty object |
| <a id="s-c026a0a816"></a>`TargetContract` | type="object"; fields=`contract_sha256`, `image_digest`, `implementation_id`, `implementation_version`, `operations`, `protocol`, `source_revision`, `transport`; additional keys=`additionalProperties`, `required` |
| <a id="s-69a5afc687"></a>`TargetInputAuthority` | type="object"; fields=`roles`, `selection`; additional keys=`additionalProperties`, `required` |
| <a id="s-73fe014af3"></a>`TargetInputRoleCount` | type="object"; fields=`count`, `role`; additional keys=`additionalProperties`, `required` |
| <a id="s-3d724492cb"></a>`TargetOperationSupport` | type="object"; fields=`operation_contract_sha256`, `operation_id`, `options_schema`, `result_kind`; additional keys=`additionalProperties`, `required` |
| <a id="s-93aa9fa485"></a>`TransformPlan` | type="object"; fields=`inputs`, `intent`, `observation_result_sha256s`, `operation_contract_sha256`, `operation_id`, `plan_sha256`, `protocol`, `target_contract_sha256`, `target_implementation_id`, `target_options`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.TargetPreflightResponse.bind_protocol](stove0-target-protocol-targetpreflightresponse-bind-protocol.md)

## Governing policies

- <a id="pa-9dc63aba2f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetPreflightResponse`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6468f6ab0ccf6a3e0c8efdeb897fb08308ebc50722a59bf63207927716044c8a -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
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
    },
    "signature": "'(*, target: stove0_target_protocol.protocol.TargetContract, plan: stove0_target_protocol.protocol.TransformPlan | stove0_target_protocol.protocol.EffectPlan) -> None'"
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetPreflightResponse",
  "unit": "export"
}
```
