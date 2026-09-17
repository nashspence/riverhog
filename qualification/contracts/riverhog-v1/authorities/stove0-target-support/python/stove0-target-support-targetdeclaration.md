# stove0_target_support.TargetDeclaration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetdeclaration:b23862c5f9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2a0da35103"></a>
- <a id="s-d6ef936742"></a>`distribution`: `stove0-target-support`
- <a id="s-855846da2e"></a>`module`: `stove0_target_support`
- <a id="s-a98d65cb81"></a>`name`: `TargetDeclaration`
- <a id="s-69b871ce2c"></a>`unit`: `export`

### Declared structure

- <a id="s-adca6e519f"></a>`kind`: `"class"`
- <a id="s-69ef8b4c5c"></a>`signature`: `"\"(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], inputs: stove0_target_protocol.protocol.TargetInputAuthority, intent: dict[str, JsonValue], target_options: dict[str, JsonValue] = <factory>) -> None\""`

#### Validated model schema

<a id="s-d8c948d624"></a>

- <a id="s-da9a390ca6"></a>`type`: `"object"`
- <a id="s-5551654952"></a>`additionalProperties`: `false`
- <a id="s-c793d167ff"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-229e7fd62b"></a>`inputs` | yes | [TargetInputAuthority](#s-4bc68ace91) |  |
| <a id="s-31da06045e"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-31a5efdabd)) |  |
| <a id="s-ba9f7c1813"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6a83987d47"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-44aa7991ad"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-31a5efdabd)) |  |

##### Definitions

- [ArtifactSelectionRef](#s-e78212d4d6)
- [JsonValue](#s-31a5efdabd)
- [TargetInputAuthority](#s-4bc68ace91)
- [TargetInputRoleCount](#s-d2b21731a7)

##### <a id="s-e78212d4d6"></a>definition `ArtifactSelectionRef`

- <a id="s-8a895b0ea0"></a>`type`: `"object"`
- <a id="s-cbf90414f7"></a>`additionalProperties`: `false`
- <a id="s-35cfa05eba"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-55e466d279"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-9f84172773"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-89fbdfaf32"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-31a5efdabd"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-4bc68ace91"></a>definition `TargetInputAuthority`

- <a id="s-6c734677e6"></a>`type`: `"object"`
- <a id="s-9a339c94ac"></a>`additionalProperties`: `false`
- <a id="s-c0f95458ec"></a>`required`: `["selection","roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a6c1f70d7e"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](#s-d2b21731a7)); minItems=1 |  |
| <a id="s-abed86fcc0"></a>`selection` | yes | [ArtifactSelectionRef](#s-e78212d4d6) |  |

##### <a id="s-d2b21731a7"></a>definition `TargetInputRoleCount`

- <a id="s-08c839ba89"></a>`type`: `"object"`
- <a id="s-62ec0f52d0"></a>`additionalProperties`: `false`
- <a id="s-5146e7912f"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-736217aac5"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-d5b43a7b76"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

## Governing policies

- <a id="pa-8c06cfcc9c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — [reference/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetDeclaration`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 19b7ce119bbb5927c9f5a1a5e50b6eb806afc63587d6e068a2c4900cf0caf6c2 -->

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
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetDeclaration",
  "unit": "export"
}
```

</details>
