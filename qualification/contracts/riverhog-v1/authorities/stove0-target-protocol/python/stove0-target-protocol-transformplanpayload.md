# stove0_target_protocol.TransformPlanPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-transformplanpayload:0c1b905a5e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d228985bec"></a>
- <a id="s-21b3508ca7"></a>`distribution`: `stove0-target-protocol`
- <a id="s-8cfe981732"></a>`module`: `stove0_target_protocol`
- <a id="s-348eae554c"></a>`name`: `TransformPlanPayload`
- <a id="s-f6bd73bb84"></a>`unit`: `export`

### Declared structure

- <a id="s-926639e2c7"></a>`kind`: `"class"`
- <a id="s-f8d43e2285"></a>`signature`: `"\"(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], inputs: stove0_target_protocol.protocol.TargetInputAuthority, intent: dict[str, JsonValue], target_options: dict[str, JsonValue] = <factory>, protocol: Literal['stove0-transform-target/v1'] = 'stove0-transform-target/v1', target_implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], observation_result_sha256s: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], ...] = ()) -> None\""`

#### Validated model schema

<a id="s-4e681e4978"></a>

- <a id="s-90d841659b"></a>`type`: `"object"`
- <a id="s-785a98f031"></a>`additionalProperties`: `false`
- <a id="s-26ea8a1f1f"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent","target_implementation_id","target_contract_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f0e263ac9d"></a>`inputs` | yes | [TargetInputAuthority](#s-e2972fa4bd) |  |
| <a id="s-6e2c56b0aa"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-3bca7204fc)) |  |
| <a id="s-2e073a28d3"></a>`observation_result_sha256s` | no | type="array"; default=[]; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-5f3d0c57e2"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-43b56c7a23"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-72ced4fbef"></a>`protocol` | no | type="string"; const="stove0-transform-target/v1"; default="stove0-transform-target/v1" |  |
| <a id="s-87db4ac482"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-dbcac0796e"></a>`target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-224fdab42d"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-3bca7204fc)) |  |

##### Definitions

- [ArtifactSelectionRef](#s-5e5e35ab5a)
- [JsonValue](#s-3bca7204fc)
- [TargetInputAuthority](#s-e2972fa4bd)
- [TargetInputRoleCount](#s-b7c586f3cd)

##### <a id="s-5e5e35ab5a"></a>definition `ArtifactSelectionRef`

- <a id="s-080d42eb73"></a>`type`: `"object"`
- <a id="s-399b13477f"></a>`additionalProperties`: `false`
- <a id="s-ecc0ce2912"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f431915035"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-3643e80cd1"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-72605dc701"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-3bca7204fc"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-e2972fa4bd"></a>definition `TargetInputAuthority`

- <a id="s-9675839cbb"></a>`type`: `"object"`
- <a id="s-a903f11ace"></a>`additionalProperties`: `false`
- <a id="s-7cb4573eed"></a>`required`: `["selection","roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3e7cc3a552"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](#s-b7c586f3cd)); minItems=1 |  |
| <a id="s-a69474ee0b"></a>`selection` | yes | [ArtifactSelectionRef](#s-5e5e35ab5a) |  |

##### <a id="s-b7c586f3cd"></a>definition `TargetInputRoleCount`

- <a id="s-cc19f76db6"></a>`type`: `"object"`
- <a id="s-c41e9e8d7a"></a>`additionalProperties`: `false`
- <a id="s-d56e1b1b8e"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8d7e060e16"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-75e49bfb61"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

## Maintained corroboration

### Related interface records

- [canonical_observation_results](stove0-target-protocol-transformplanpayload-canonical-observation-results.md)

## Governing policies

- <a id="pa-891637f40b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TransformPlanPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fd41d64f02752f8c5969464341c5299a22103f74eb1bb3cd6eb5c09100697d5f -->

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
        "JsonValue": {},
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
        "target_contract_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], inputs: stove0_target_protocol.protocol.TargetInputAuthority, intent: dict[str, JsonValue], target_options: dict[str, JsonValue] = <factory>, protocol: Literal['stove0-transform-target/v1'] = 'stove0-transform-target/v1', target_implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], observation_result_sha256s: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], ...] = ()) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TransformPlanPayload",
  "unit": "export"
}
```

</details>
