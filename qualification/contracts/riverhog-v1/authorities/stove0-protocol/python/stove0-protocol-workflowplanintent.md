# stove0_protocol.WorkflowPlanIntent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowplanintent:0cac2ec3e5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6f4992aa06"></a>
- <a id="s-7e6f18d68f"></a>`distribution`: `stove0-protocol`
- <a id="s-7c44eb7e20"></a>`module`: `stove0_protocol`
- <a id="s-2f3063eeb2"></a>`name`: `WorkflowPlanIntent`
- <a id="s-c87970994e"></a>`unit`: `export`

### Declared structure

- <a id="s-f7f756ab42"></a>`kind`: `"class"`
- <a id="s-a616f91be3"></a>`signature`: `"\"(*, operation: stove0_protocol.models.OperationRef, result_kind: Literal['collection', 'external-effect'] = 'collection', target_registration_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], requested_target_options: dict[str, JsonValue] = <factory>, input_retrieval_policy: Literal['available-only', 'allow'] = 'available-only', retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, output_policy: dict[str, JsonValue] = <factory>) -> None\""`

#### Validated model schema

<a id="s-5c455d3d8b"></a>

- <a id="s-e4f76395f2"></a>`type`: `"object"`
- <a id="s-317ea20463"></a>`additionalProperties`: `false`
- <a id="s-acdb2b0202"></a>`required`: `["operation","target_registration_id","target_contract_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f68ba11817"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-e739133219"></a>`operation` | yes | [OperationRef](#s-429ed7f27c) |  |
| <a id="s-4110fe6ff5"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](#s-7ed2d99a03)) |  |
| <a id="s-f9b8ce04e4"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](#s-7ed2d99a03)) |  |
| <a id="s-9e24211777"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-67d7f743ca"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-eb4cc9eb3f"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-e95da0ad87"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-503dea0230"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |

##### Definitions

- [JsonValue](#s-7ed2d99a03)
- [OperationRef](#s-429ed7f27c)

##### <a id="s-7ed2d99a03"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-429ed7f27c"></a>definition `OperationRef`

- <a id="s-ececb7d2f0"></a>`type`: `"object"`
- <a id="s-951ccc997a"></a>`additionalProperties`: `false`
- <a id="s-0f4c137e81"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-eaa5f8a918"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-caa7dfd9c3"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [from_plan](stove0-protocol-workflowplanintent-from-plan.md)
- [materialize](stove0-protocol-workflowplanintent-materialize.md)
- [validate_retirement](stove0-protocol-workflowplanintent-validate-retirement.md)

## Governing policies

- <a id="pa-72e38e0b01"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPlanIntent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: db00d9b311b0379dfae608f0d6d79d69194631460fd5db802419ccc1eb8e453f -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "JsonValue": {},
        "OperationRef": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "id",
            "sha256"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "input_retrieval_policy": {
          "default": "available-only",
          "enum": [
            "available-only",
            "allow"
          ],
          "type": "string"
        },
        "operation": {
          "$ref": "#/$defs/OperationRef"
        },
        "output_policy": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "type": "object"
        },
        "requested_target_options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "type": "object"
        },
        "result_kind": {
          "default": "collection",
          "enum": [
            "collection",
            "external-effect"
          ],
          "type": "string"
        },
        "retirement_grace_seconds": {
          "default": 0,
          "minimum": 0,
          "type": "integer"
        },
        "retirement_policy": {
          "default": "retain",
          "enum": [
            "retain",
            "retire-after-verified-output"
          ],
          "type": "string"
        },
        "target_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "target_registration_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$",
          "type": "string"
        }
      },
      "required": [
        "operation",
        "target_registration_id",
        "target_contract_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, operation: stove0_protocol.models.OperationRef, result_kind: Literal['collection', 'external-effect'] = 'collection', target_registration_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], requested_target_options: dict[str, JsonValue] = <factory>, input_retrieval_policy: Literal['available-only', 'allow'] = 'available-only', retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, output_policy: dict[str, JsonValue] = <factory>) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "WorkflowPlanIntent",
  "unit": "export"
}
```

</details>
