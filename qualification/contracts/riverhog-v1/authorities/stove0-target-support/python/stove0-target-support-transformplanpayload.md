# stove0_target_support.TransformPlanPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-transformplanpayload:1354eb7ea7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b0066ced20"></a>
- <a id="s-681ef6cb94"></a>`distribution`: `stove0-target-support`
- <a id="s-7b00abc452"></a>`module`: `stove0_target_support`
- <a id="s-3337072a9a"></a>`name`: `TransformPlanPayload`
- <a id="s-ff9824296b"></a>`unit`: `export`

### Declared structure

- <a id="s-bc58eb8909"></a>`kind`: `"class"`
- <a id="s-4dcb2c99a4"></a>`signature`: `"\"(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], inputs: stove0_target_protocol.protocol.TargetInputAuthority, intent: dict[str, JsonValue], target_options: dict[str, JsonValue] = <factory>, protocol: Literal['stove0-transform-target/v1'] = 'stove0-transform-target/v1', target_implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], target_descriptor_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], observation_result_sha256s: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], ...] = ()) -> None\""`

#### Validated model schema

<a id="s-2732d56d71"></a>

- <a id="s-d6148d723f"></a>`type`: `"object"`
- <a id="s-6d8d514dc6"></a>`additionalProperties`: `false`
- <a id="s-f97fdad5e3"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_descriptor_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6847f8513b"></a>`inputs` | yes | [TargetInputAuthority](#s-e393d5abb7) |  |
| <a id="s-cd7afd96c9"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-109ea7f1b5)) |  |
| <a id="s-411be1b274"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-8c011c21bc"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-57150a0b2d"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-258b3b94ab"></a>`protocol` | no | type="string"; const="stove0-transform-target/v1"; default="stove0-transform-target/v1" |  |
| <a id="s-9c6bf1c948"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b649dc8b8b"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-f4d6269c7a"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-109ea7f1b5)) |  |

##### Definitions

- [ArtifactSelectionRef](#s-5fc5f00b09)
- [JsonValue](#s-109ea7f1b5)
- [NonnegativeDecimal](#s-e9e34a3b75)
- [TargetInputAuthority](#s-e393d5abb7)
- [TargetInputRoleCount](#s-98a56b07b1)

##### <a id="s-5fc5f00b09"></a>definition `ArtifactSelectionRef`

- <a id="s-83805b756b"></a>`type`: `"object"`
- <a id="s-18d0a772d5"></a>`additionalProperties`: `false`
- <a id="s-f0482e6922"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e221a27b41"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-8a22320850"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d1b6c87e22"></a>`total_bytes` | yes | [NonnegativeDecimal](#s-e9e34a3b75); ge=0 |  |

##### <a id="s-109ea7f1b5"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-e9e34a3b75"></a>definition `NonnegativeDecimal`

- <a id="s-305a45de69"></a>`type`: `"string"`
- <a id="s-b2953906e9"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

##### <a id="s-e393d5abb7"></a>definition `TargetInputAuthority`

- <a id="s-b8e4ea17ec"></a>`type`: `"object"`
- <a id="s-9f4a2e421e"></a>`additionalProperties`: `false`
- <a id="s-13ed9e1344"></a>`required`: `["selection","roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-da8b657f62"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](#s-98a56b07b1)); minItems=1 |  |
| <a id="s-c558029922"></a>`selection` | yes | [ArtifactSelectionRef](#s-5fc5f00b09) |  |

##### <a id="s-98a56b07b1"></a>definition `TargetInputRoleCount`

- <a id="s-efaa5031e7"></a>`type`: `"object"`
- <a id="s-2c2a597bc8"></a>`additionalProperties`: `false`
- <a id="s-9799c0cbee"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3bd3a8b7b4"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-432098b292"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

## Maintained corroboration

### Related interface records

- [canonical_observation_results](stove0-target-support-transformplanpayload-canonical-observation-results.md)

## Governing policies

- <a id="pa-e30b3b6df6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TransformPlanPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2dccea0d03b5c8242c351827f8d612bf9b8fbc5fb9e424d7978fc0d1df96813b -->

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
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 0
            }
          },
          "required": [
            "selection_sha256",
            "artifact_count",
            "total_bytes"
          ],
          "type": "object"
        },
        "JsonValue": {},
        "NonnegativeDecimal": {
          "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
          "type": "string"
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
        }
      },
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
        "target_descriptor_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], inputs: stove0_target_protocol.protocol.TargetInputAuthority, intent: dict[str, JsonValue], target_options: dict[str, JsonValue] = <factory>, protocol: Literal['stove0-transform-target/v1'] = 'stove0-transform-target/v1', target_implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], target_descriptor_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], observation_result_sha256s: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], ...] = ()) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TransformPlanPayload",
  "unit": "export"
}
```

</details>
