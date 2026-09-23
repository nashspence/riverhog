# stove0_target_protocol.TargetPreflightResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetpreflightresponse:97c1b950c1 -->

Exact externally visible contract owned by this contract element.

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
- <a id="s-9363613723"></a>`signature`: `"'(*, descriptor: stove0_target_protocol.protocol.TargetDescriptor, plan: stove0_target_protocol.protocol.TransformPlan \| stove0_target_protocol.protocol.EffectPlan) -> None'"`

#### Validated model schema

<a id="s-4758c4be59"></a>

- <a id="s-64a96bafdd"></a>`type`: `"object"`
- <a id="s-d03151f17f"></a>`additionalProperties`: `false`
- <a id="s-bbefe9b13c"></a>`required`: `["descriptor","plan"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9f19ecf8b3"></a>`descriptor` | yes | [TargetDescriptor](#s-2d4babd363) |  |
| <a id="s-adc278d8ef"></a>`plan` | yes | discriminator={"mapping":{"stove0-effect-target/v1":"#/$defs/EffectPlan","stove0-transform-target/v1":"#/$defs/TransformPlan"},"propertyName":"protocol"}; oneOf=[([TransformPlan](#s-93aa9fa485)); ([EffectPlan](#s-76c6471c9f))] |  |

##### Definitions

- [ArtifactSelectionRef](#s-e634dd1750)
- [EffectPlan](#s-76c6471c9f)
- [JsonSchemaValidationProfile](#s-12d06c88a6)
- [JsonValue](#s-9f4f47f35d)
- [TargetDescriptor](#s-2d4babd363)
- [TargetInputAuthority](#s-69a5afc687)
- [TargetInputRoleCount](#s-73fe014af3)
- [TargetOperationSupport](#s-3d724492cb)
- [TransformPlan](#s-93aa9fa485)

##### <a id="s-e634dd1750"></a>definition `ArtifactSelectionRef`

- <a id="s-a14540230c"></a>`type`: `"object"`
- <a id="s-437705e5d9"></a>`additionalProperties`: `false`
- <a id="s-26e1a1b103"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6d65feb2a1"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-5b856de9c7"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7cfcf4302f"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-76c6471c9f"></a>definition `EffectPlan`

- <a id="s-3220f61798"></a>`type`: `"object"`
- <a id="s-93d67708a9"></a>`additionalProperties`: `false`
- <a id="s-9ab22ccfac"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_descriptor_sha256","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a6fe3038c9"></a>`inputs` | yes | [TargetInputAuthority](#s-69a5afc687) |  |
| <a id="s-ee94d5e2e2"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-9f4f47f35d)) |  |
| <a id="s-35b5417d5b"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-aa37fbf9c7"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-677d14119f"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-c2ba9a3b86"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b41ae31985"></a>`protocol` | no | type="string"; const="stove0-effect-target/v1"; default="stove0-effect-target/v1" |  |
| <a id="s-d364a30fce"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e809d26895"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-57affba3de"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-9f4f47f35d)) |  |

##### <a id="s-12d06c88a6"></a>definition `JsonSchemaValidationProfile`

- <a id="s-dc24b39e1f"></a>`type`: `"object"`
- <a id="s-260ee00050"></a>`additionalProperties`: `false`
- <a id="s-117df67b9d"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9c6975abec"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-dd65a6c3bc"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-12a68f6992"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0a4eb71251"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1efafb7111"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-9f4f47f35d)) |  |

##### <a id="s-9f4f47f35d"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-2d4babd363"></a>definition `TargetDescriptor`

- <a id="s-04507a1053"></a>`type`: `"object"`
- <a id="s-3e2af92699"></a>`additionalProperties`: `false`
- <a id="s-b700f75939"></a>`required`: `["implementation_id","implementation_version","source_revision","image_digest","operations","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f824a99761"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ddac971a50"></a>`image_digest` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d7bd8d89e6"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-217028644f"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1 |  |
| <a id="s-3771b06fc9"></a>`operations` | yes | type="array"; items=([TargetOperationSupport](#s-3d724492cb)); minItems=1 |  |
| <a id="s-c93ae46a00"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1" |  |
| <a id="s-c8a57fc81a"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-1868b11828"></a>`transport` | no | type="string"; const="riverhog-capability/v1"; default="riverhog-capability/v1" |  |

##### <a id="s-69a5afc687"></a>definition `TargetInputAuthority`

- <a id="s-c49138d78f"></a>`type`: `"object"`
- <a id="s-0209559c99"></a>`additionalProperties`: `false`
- <a id="s-f889aed713"></a>`required`: `["selection","roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-456ecfd675"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](#s-73fe014af3)); minItems=1 |  |
| <a id="s-e49e9ac778"></a>`selection` | yes | [ArtifactSelectionRef](#s-e634dd1750) |  |

##### <a id="s-73fe014af3"></a>definition `TargetInputRoleCount`

- <a id="s-9a8bd04463"></a>`type`: `"object"`
- <a id="s-7838d7bf1b"></a>`additionalProperties`: `false`
- <a id="s-ae43d45677"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e5162615e3"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-7db6712502"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-3d724492cb"></a>definition `TargetOperationSupport`

- <a id="s-6b4bebe566"></a>`type`: `"object"`
- <a id="s-d88fc6812d"></a>`additionalProperties`: `false`
- <a id="s-56de7cd24e"></a>`required`: `["operation_id","operation_contract_sha256","options_schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ddd757f4af"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e6c8c05a19"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-63181f374e"></a>`options_schema` | yes | [JsonSchemaValidationProfile](#s-12d06c88a6) |  |
| <a id="s-c17bde5837"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |

##### <a id="s-93aa9fa485"></a>definition `TransformPlan`

- <a id="s-9ddea3a19a"></a>`type`: `"object"`
- <a id="s-6fdc34c411"></a>`additionalProperties`: `false`
- <a id="s-49fe156ad7"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_descriptor_sha256","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b167a557b9"></a>`inputs` | yes | [TargetInputAuthority](#s-69a5afc687) |  |
| <a id="s-99edb50390"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-9f4f47f35d)) |  |
| <a id="s-b46a5967bb"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-e3719b3f13"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ece19e4e63"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-fb1225d9ec"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-00880ef336"></a>`protocol` | no | type="string"; const="stove0-transform-target/v1"; default="stove0-transform-target/v1" |  |
| <a id="s-873eb8f1d2"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b82319aa07"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-19e36a79f6"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-9f4f47f35d)) |  |

## Maintained corroboration

### Related interface records

- [bind_protocol](stove0-target-protocol-targetpreflightresponse-bind-protocol.md)

## Governing policies

- <a id="pa-9dc63aba2f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetPreflightResponse`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cd1583e41b6c9b17db33a2691b9657cb247050aeee49520f18b7694fb5afbbb6 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactSelectionRef": {
          "additionalProperties": false,
          "properties": {
            "artifact_count": {
              "minimum": 1,
              "type": "integer"
            },
            "selection_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "total_bytes": {
              "minimum": 0,
              "type": "integer"
            }
          },
          "required": [
            "selection_sha256",
            "artifact_count",
            "total_bytes"
          ],
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
              "type": "object"
            },
            "observation_result_sha256s": {
              "default": [],
              "items": {
                "pattern": "^[0-9a-f]{64}$",
                "type": "string"
              },
              "type": "array"
            },
            "operation_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "operation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "protocol": {
              "const": "stove0-effect-target/v1",
              "default": "stove0-effect-target/v1",
              "type": "string"
            },
            "target_descriptor_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "target_implementation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "target_options": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            }
          },
          "required": [
            "operation_id",
            "operation_contract_sha256",
            "inputs",
            "intent",
            "target_implementation_id",
            "target_descriptor_sha256",
            "plan_sha256"
          ],
          "type": "object"
        },
        "JsonSchemaValidationProfile": {
          "additionalProperties": false,
          "properties": {
            "dialect": {
              "const": "https://json-schema.org/draft/2020-12/schema",
              "default": "https://json-schema.org/draft/2020-12/schema",
              "type": "string"
            },
            "format_policy": {
              "const": "annotation-only",
              "default": "annotation-only",
              "type": "string"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "profile_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "schema": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            }
          },
          "required": [
            "id",
            "profile_sha256",
            "schema"
          ],
          "type": "object"
        },
        "JsonValue": {},
        "TargetDescriptor": {
          "additionalProperties": false,
          "properties": {
            "descriptor_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "image_digest": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "implementation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "implementation_version": {
              "maxLength": 120,
              "minLength": 1,
              "type": "string"
            },
            "operations": {
              "items": {
                "$ref": "#/$defs/TargetOperationSupport"
              },
              "minItems": 1,
              "type": "array"
            },
            "protocol": {
              "default": "stove0-transform-target/v1",
              "enum": [
                "stove0-transform-target/v1",
                "stove0-effect-target/v1"
              ],
              "type": "string"
            },
            "source_revision": {
              "maxLength": 200,
              "minLength": 1,
              "type": "string"
            },
            "transport": {
              "const": "riverhog-capability/v1",
              "default": "riverhog-capability/v1",
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
          "type": "object"
        },
        "TargetInputAuthority": {
          "additionalProperties": false,
          "properties": {
            "roles": {
              "items": {
                "$ref": "#/$defs/TargetInputRoleCount"
              },
              "minItems": 1,
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
          "type": "object"
        },
        "TargetInputRoleCount": {
          "additionalProperties": false,
          "properties": {
            "count": {
              "minimum": 1,
              "type": "integer"
            },
            "role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            }
          },
          "required": [
            "role",
            "count"
          ],
          "type": "object"
        },
        "TargetOperationSupport": {
          "additionalProperties": false,
          "properties": {
            "operation_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "operation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
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
              "type": "string"
            }
          },
          "required": [
            "operation_id",
            "operation_contract_sha256",
            "options_schema"
          ],
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
              "type": "object"
            },
            "observation_result_sha256s": {
              "default": [],
              "items": {
                "pattern": "^[0-9a-f]{64}$",
                "type": "string"
              },
              "type": "array"
            },
            "operation_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "operation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "protocol": {
              "const": "stove0-transform-target/v1",
              "default": "stove0-transform-target/v1",
              "type": "string"
            },
            "target_descriptor_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "target_implementation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "target_options": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            }
          },
          "required": [
            "operation_id",
            "operation_contract_sha256",
            "inputs",
            "intent",
            "target_implementation_id",
            "target_descriptor_sha256",
            "plan_sha256"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "descriptor": {
          "$ref": "#/$defs/TargetDescriptor"
        },
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
          ]
        }
      },
      "required": [
        "descriptor",
        "plan"
      ],
      "type": "object"
    },
    "signature": "'(*, descriptor: stove0_target_protocol.protocol.TargetDescriptor, plan: stove0_target_protocol.protocol.TransformPlan | stove0_target_protocol.protocol.EffectPlan) -> None'"
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetPreflightResponse",
  "unit": "export"
}
```

</details>
