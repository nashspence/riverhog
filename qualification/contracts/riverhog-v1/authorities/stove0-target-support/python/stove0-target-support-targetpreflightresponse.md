# stove0_target_support.TargetPreflightResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetpreflightresponse:cb5681b311 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fd7781d208"></a>
- <a id="s-73a5d587ed"></a>`distribution`: `stove0-target-support`
- <a id="s-3fb9905ddd"></a>`module`: `stove0_target_support`
- <a id="s-cea0734902"></a>`name`: `TargetPreflightResponse`
- <a id="s-7c62edc6c6"></a>`unit`: `export`

### Declared structure

- <a id="s-9812cdc6ea"></a>`kind`: `"class"`
- <a id="s-a1290e0254"></a>`signature`: `"'(*, target: stove0_target_protocol.protocol.TargetContract, plan: stove0_target_protocol.protocol.TransformPlan \| stove0_target_protocol.protocol.EffectPlan) -> None'"`

#### Validated model schema

<a id="s-f3ea57d221"></a>

- <a id="s-b1e8d4b44a"></a>`type`: `"object"`
- <a id="s-4c72f56431"></a>`additionalProperties`: `false`
- <a id="s-1746c5ea9f"></a>`required`: `["target","plan"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-67122c7960"></a>`plan` | yes | discriminator={"mapping":{"stove0-effect-target/v1":"#/$defs/EffectPlan","stove0-transform-target/v1":"#/$defs/TransformPlan"},"propertyName":"protocol"}; oneOf=[([TransformPlan](#s-a567e96002)); ([EffectPlan](#s-7749ef86f4))] |  |
| <a id="s-46c2459f47"></a>`target` | yes | [TargetContract](#s-ae5057e7c3) |  |

##### Definitions

- [ArtifactSelectionRef](#s-575c19c319)
- [EffectPlan](#s-7749ef86f4)
- [JsonSchemaDocument](#s-f8e1bf80e8)
- [JsonValue](#s-fdd6b5bb87)
- [TargetContract](#s-ae5057e7c3)
- [TargetInputAuthority](#s-09f382d493)
- [TargetInputRoleCount](#s-f2000ee207)
- [TargetOperationSupport](#s-95fe3a7a5b)
- [TransformPlan](#s-a567e96002)

##### <a id="s-575c19c319"></a>definition `ArtifactSelectionRef`

- <a id="s-d0503de797"></a>`type`: `"object"`
- <a id="s-e5c3016ec2"></a>`additionalProperties`: `false`
- <a id="s-f107935fa3"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a55d05ca42"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-9e9f0f4016"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ece6fe31c3"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-7749ef86f4"></a>definition `EffectPlan`

- <a id="s-1512ca2314"></a>`type`: `"object"`
- <a id="s-5532c6879e"></a>`additionalProperties`: `false`
- <a id="s-403d6ea488"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_contract_sha256","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a9247837c7"></a>`inputs` | yes | [TargetInputAuthority](#s-09f382d493) |  |
| <a id="s-6068f2a7e1"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-fdd6b5bb87)) |  |
| <a id="s-0431da507a"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-e4d78e974c"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-60361fefc1"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-3b2d392768"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0eb0a0b0e7"></a>`protocol` | no | type="string"; const="stove0-effect-target/v1"; default="stove0-effect-target/v1" |  |
| <a id="s-a5fdd944fe"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ce79998137"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0ae892e688"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-fdd6b5bb87)) |  |

##### <a id="s-f8e1bf80e8"></a>definition `JsonSchemaDocument`

- <a id="s-8e333ccc1c"></a>`type`: `"object"`
- <a id="s-7e27fd87e7"></a>`additionalProperties`: `false`
- <a id="s-6b52d4f659"></a>`required`: `["id","sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-da9463089c"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-73722d944c"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-815b4d407d"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-edea26098b"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-fdd6b5bb87)) |  |
| <a id="s-8c635f0faa"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-fdd6b5bb87"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-ae5057e7c3"></a>definition `TargetContract`

- <a id="s-184144250b"></a>`type`: `"object"`
- <a id="s-e8734bf0da"></a>`additionalProperties`: `false`
- <a id="s-a161ab9fdf"></a>`required`: `["implementation_id","implementation_version","source_revision","image_digest","operations","contract_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-85c83faedd"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5258a43b36"></a>`image_digest` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-95b95ffe65"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-338bc48d5a"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1 |  |
| <a id="s-eb8548b9a2"></a>`operations` | yes | type="array"; items=([TargetOperationSupport](#s-95fe3a7a5b)); minItems=1 |  |
| <a id="s-422725cffb"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1" |  |
| <a id="s-cb2b1fa18f"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-4a961765d9"></a>`transport` | no | type="string"; const="riverhog-capability/v1"; default="riverhog-capability/v1" |  |

##### <a id="s-09f382d493"></a>definition `TargetInputAuthority`

- <a id="s-d98207a7e6"></a>`type`: `"object"`
- <a id="s-9f8b062b5c"></a>`additionalProperties`: `false`
- <a id="s-63cf162570"></a>`required`: `["selection","roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-545193afc7"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](#s-f2000ee207)); minItems=1 |  |
| <a id="s-41e5994e0c"></a>`selection` | yes | [ArtifactSelectionRef](#s-575c19c319) |  |

##### <a id="s-f2000ee207"></a>definition `TargetInputRoleCount`

- <a id="s-cee0f73c25"></a>`type`: `"object"`
- <a id="s-69c5bd42c1"></a>`additionalProperties`: `false`
- <a id="s-12df129e48"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a435ec5bb1"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-a57ad24c55"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-95fe3a7a5b"></a>definition `TargetOperationSupport`

- <a id="s-b2720e823b"></a>`type`: `"object"`
- <a id="s-09c8ffc7e2"></a>`additionalProperties`: `false`
- <a id="s-7d84795b9e"></a>`required`: `["operation_id","operation_contract_sha256","options_schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-856bfc1d2b"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-00202498b2"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-1f6a198fe0"></a>`options_schema` | yes | [JsonSchemaDocument](#s-f8e1bf80e8) |  |
| <a id="s-38ff861273"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |

##### <a id="s-a567e96002"></a>definition `TransformPlan`

- <a id="s-ffb06da606"></a>`type`: `"object"`
- <a id="s-bd09b8ce1f"></a>`additionalProperties`: `false`
- <a id="s-9bdfc85a6d"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_contract_sha256","plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-688397a67b"></a>`inputs` | yes | [TargetInputAuthority](#s-09f382d493) |  |
| <a id="s-f08e189c04"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-fdd6b5bb87)) |  |
| <a id="s-0c95c7066b"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-c4a78e19e5"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fa13f9e457"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d75c5248da"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a0776b59c7"></a>`protocol` | no | type="string"; const="stove0-transform-target/v1"; default="stove0-transform-target/v1" |  |
| <a id="s-31dcb29484"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-548d5509e4"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-8ffa2c9c6f"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-fdd6b5bb87)) |  |

## Maintained corroboration

### Related interface records

- [bind_protocol](stove0-target-support-targetpreflightresponse-bind-protocol.md)

## Governing policies

- <a id="pa-6d30fd4c40"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [reference/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetPreflightResponse`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7484b2a8f51ad664ef6718bda399381e9c693803cd4e387176ce786156d2752e -->

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
            "target_contract_sha256": {
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
            "target_contract_sha256",
            "plan_sha256"
          ],
          "type": "object"
        },
        "JsonSchemaDocument": {
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
            "schema": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "id",
            "sha256",
            "schema"
          ],
          "type": "object"
        },
        "JsonValue": {},
        "TargetContract": {
          "additionalProperties": false,
          "properties": {
            "contract_sha256": {
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
            "contract_sha256"
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
              "$ref": "#/$defs/JsonSchemaDocument"
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
            "target_contract_sha256": {
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
            "target_contract_sha256",
            "plan_sha256"
          ],
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
          ]
        },
        "target": {
          "$ref": "#/$defs/TargetContract"
        }
      },
      "required": [
        "target",
        "plan"
      ],
      "type": "object"
    },
    "signature": "'(*, target: stove0_target_protocol.protocol.TargetContract, plan: stove0_target_protocol.protocol.TransformPlan | stove0_target_protocol.protocol.EffectPlan) -> None'"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetPreflightResponse",
  "unit": "export"
}
```

</details>
