# stove0_target_protocol.TargetDeclaration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetdeclaration:15a1d49007 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b7498f526c"></a>
- <a id="s-2b2ba036d5"></a>`distribution`: `stove0-target-protocol`
- <a id="s-ecc21766d5"></a>`module`: `stove0_target_protocol`
- <a id="s-7fb2d4675d"></a>`name`: `TargetDeclaration`
- <a id="s-d03c570747"></a>`unit`: `export`

### Declared structure

- <a id="s-b367b04639"></a>`kind`: `"class"`
- <a id="s-5741c28e26"></a>`signature`: `"\"(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], inputs: stove0_target_protocol.protocol.TargetInputAuthority, intent: dict[str, JsonValue], target_options: dict[str, JsonValue] = <factory>) -> None\""`

#### Validated model schema

<a id="s-37219e2d2e"></a>

- <a id="s-22fe6cb854"></a>`type`: `"object"`
- <a id="s-88bf54dcec"></a>`additionalProperties`: `false`
- <a id="s-9f8ba67062"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a287e0badd"></a>`inputs` | yes | [TargetInputAuthority](#s-894c0460ed) |  |
| <a id="s-9cbcf14317"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-b02bf50d68)) |  |
| <a id="s-74f40fc5cc"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-dd270463c2"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d58529fd80"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-b02bf50d68)) |  |

##### Definitions

- [ArtifactSelectionRef](#s-84dc83c9f1)
- [JsonValue](#s-b02bf50d68)
- [TargetInputAuthority](#s-894c0460ed)
- [TargetInputRoleCount](#s-7b067f8b3b)

##### <a id="s-84dc83c9f1"></a>definition `ArtifactSelectionRef`

- <a id="s-19b9259da5"></a>`type`: `"object"`
- <a id="s-e5a3190c25"></a>`additionalProperties`: `false`
- <a id="s-4fee02dfb7"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ca327a2768"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-9b5aab4100"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4fa7cdd041"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-b02bf50d68"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-894c0460ed"></a>definition `TargetInputAuthority`

- <a id="s-32b8397d91"></a>`type`: `"object"`
- <a id="s-e1ca4479de"></a>`additionalProperties`: `false`
- <a id="s-cf27174d82"></a>`required`: `["selection","roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fb6f116f73"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](#s-7b067f8b3b)); minItems=1 |  |
| <a id="s-2c7a152f71"></a>`selection` | yes | [ArtifactSelectionRef](#s-84dc83c9f1) |  |

##### <a id="s-7b067f8b3b"></a>definition `TargetInputRoleCount`

- <a id="s-6dc4b36c8f"></a>`type`: `"object"`
- <a id="s-9dd234c4f1"></a>`additionalProperties`: `false`
- <a id="s-54a382c71d"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-21eac204a0"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-62514dd74a"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

## Governing policies

- <a id="pa-996e034c1c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetDeclaration`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ec161219a3877473ceb599e7e3ed9c91e0af9e915b6a3e1db3c113f2d18cfe74 -->

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
        "operation_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "operation_id": {
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
        "intent"
      ],
      "type": "object"
    },
    "signature": "\"(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], inputs: stove0_target_protocol.protocol.TargetInputAuthority, intent: dict[str, JsonValue], target_options: dict[str, JsonValue] = <factory>) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetDeclaration",
  "unit": "export"
}
```

</details>
