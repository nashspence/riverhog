# stove0_core.EvaluationChild

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationchild:a4ecbd76ce -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9dec0e3508"></a>
- <a id="s-974333000c"></a>`distribution`: `stove0-server`
- <a id="s-22cc756702"></a>`module`: `stove0_core`
- <a id="s-d93d9fefff"></a>`name`: `EvaluationChild`
- <a id="s-57ff840119"></a>`unit`: `export`

### Declared structure

- <a id="s-93d38cd0b0"></a>`kind`: `"class"`
- <a id="s-e4eb2802c5"></a>`signature`: `"\"(*, variant_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], work_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], state: Literal['pending', 'active', 'complete', 'inapplicable', 'failed', 'canceled'] = 'pending', output: stove0_target_protocol.protocol.OutputCollectionRef \| None = None) -> None\""`

#### Validated model schema

<a id="s-dfc1fb9df4"></a>

- <a id="s-3eb81de407"></a>`type`: `"object"`
- <a id="s-5a1ae6b115"></a>`additionalProperties`: `false`
- <a id="s-5496397e42"></a>`required`: `["variant_id","work_id"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6391915fed"></a>`output` | no | anyOf=[([OutputCollectionRef](#s-e9b72407fe)); (type="null")]; default=null |  |
| <a id="s-61acf207ce"></a>`state` | no | type="string"; enum=["pending","active","complete","inapplicable","failed","canceled"]; default="pending" |  |
| <a id="s-e87ab2d54d"></a>`variant_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-d54cafd583"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [CollectionId](#s-2e65c4ee9e)
- [OutputCollectionRef](#s-e9b72407fe)

##### <a id="s-2e65c4ee9e"></a>definition `CollectionId`

- <a id="s-b309b406f1"></a>`type`: `"integer"`
- <a id="s-4fbdd437da"></a>`minimum`: `1`

##### <a id="s-e9b72407fe"></a>definition `OutputCollectionRef`

- <a id="s-a4240b6be7"></a>`type`: `"object"`
- <a id="s-97cad6d2d6"></a>`additionalProperties`: `false`
- <a id="s-48f99be769"></a>`required`: `["collection_id","archive_root_sha256","content_identity","derivation_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-59479bae39"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7b01ac9189"></a>`collection_id` | yes | [CollectionId](#s-2e65c4ee9e) |  |
| <a id="s-290555e3cd"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-59c175fd64"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [validate_output](stove0-core-evaluationchild-validate-output.md)

## Governing policies

- <a id="pa-c6c927329e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.EvaluationChild`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 51f0dff1bf69250ca5c4a227e97ca3c036800c99bcd32eb2af2f89df13b992aa -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CollectionId": {
          "minimum": 1,
          "type": "integer"
        },
        "OutputCollectionRef": {
          "additionalProperties": false,
          "properties": {
            "archive_root_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "content_identity": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "derivation_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "collection_id",
            "archive_root_sha256",
            "content_identity",
            "derivation_sha256"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "output": {
          "anyOf": [
            {
              "$ref": "#/$defs/OutputCollectionRef"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "state": {
          "default": "pending",
          "enum": [
            "pending",
            "active",
            "complete",
            "inapplicable",
            "failed",
            "canceled"
          ],
          "type": "string"
        },
        "variant_id": {
          "maxLength": 160,
          "minLength": 1,
          "type": "string"
        },
        "work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "variant_id",
        "work_id"
      ],
      "type": "object"
    },
    "signature": "\"(*, variant_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], work_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], state: Literal['pending', 'active', 'complete', 'inapplicable', 'failed', 'canceled'] = 'pending', output: stove0_target_protocol.protocol.OutputCollectionRef | None = None) -> None\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "EvaluationChild",
  "unit": "export"
}
```

</details>
