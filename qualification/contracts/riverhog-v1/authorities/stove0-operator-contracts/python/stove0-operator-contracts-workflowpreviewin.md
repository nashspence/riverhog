# stove0_operator_contracts.WorkflowPreviewIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workflowpreviewin:58e3423444 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-09bdb29e6d"></a>
- <a id="s-76c97a5b30"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-9b585e3eaa"></a>`module`: `stove0_operator_contracts`
- <a id="s-8846457f5f"></a>`name`: `WorkflowPreviewIn`
- <a id="s-ad054de7e7"></a>`unit`: `export`

### Declared structure

- <a id="s-fdeee8e80a"></a>`kind`: `"class"`
- <a id="s-25f393fa2c"></a>`signature`: `"'(*, recipe_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], recipe_revision: Annotated[int \| None, Ge(ge=1)] = None, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootRef, ...], MinLen(min_length=1)], effective_intent: dict[str, JsonValue] = <factory>) -> None'"`

#### Validated model schema

<a id="s-d04d5dab91"></a>

- <a id="s-3caace8cca"></a>`type`: `"object"`
- <a id="s-bc534b234b"></a>`additionalProperties`: `false`
- <a id="s-6d0f6aa916"></a>`required`: `["recipe_id","inputs"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2d9430f995"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-e10ebafa23)) |  |
| <a id="s-21bef1f98d"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](#s-c90b432181)); minItems=1 |  |
| <a id="s-f25e9c76e6"></a>`recipe_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-0a6f3ccb15"></a>`recipe_revision` | no | anyOf=(type="integer"; minimum=1) \| (type="null"); default=null |  |

##### Definitions

- [CollectionId](#s-01b0583462)
- [CollectionRootRef](#s-c90b432181)
- [JsonValue](#s-e10ebafa23)

##### <a id="s-01b0583462"></a>definition `CollectionId`

- <a id="s-6dbfbce98c"></a>`type`: `"integer"`
- <a id="s-a54a678d2d"></a>`minimum`: `1`

##### <a id="s-c90b432181"></a>definition `CollectionRootRef`

- <a id="s-37d5864f74"></a>`type`: `"object"`
- <a id="s-c87665492d"></a>`additionalProperties`: `false`
- <a id="s-e99a5b2fff"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-120dcc48e5"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bc0d1b495a"></a>`collection_id` | yes | [CollectionId](#s-01b0583462) |  |
| <a id="s-dd03cb5e45"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-e10ebafa23"></a>definition `JsonValue`

- Accepts: any JSON value.

## Maintained corroboration

### Related interface records

- [canonical_inputs](stove0-operator-contracts-workflowpreviewin-canonical-inputs.md)

## Governing policies

- <a id="pa-acc4cce580"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkflowPreviewIn`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ad894a835d2bdd709341f26febabfdd3c23ff8ee3b6dcddf550cc1f9862b002e -->

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
        "CollectionRootRef": {
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
            }
          },
          "required": [
            "collection_id",
            "archive_root_sha256",
            "content_identity"
          ],
          "type": "object"
        },
        "JsonValue": {}
      },
      "additionalProperties": false,
      "properties": {
        "effective_intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "type": "object"
        },
        "inputs": {
          "items": {
            "$ref": "#/$defs/CollectionRootRef"
          },
          "minItems": 1,
          "type": "array"
        },
        "recipe_id": {
          "maxLength": 160,
          "minLength": 1,
          "type": "string"
        },
        "recipe_revision": {
          "anyOf": [
            {
              "minimum": 1,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        }
      },
      "required": [
        "recipe_id",
        "inputs"
      ],
      "type": "object"
    },
    "signature": "'(*, recipe_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], recipe_revision: Annotated[int | None, Ge(ge=1)] = None, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootRef, ...], MinLen(min_length=1)], effective_intent: dict[str, JsonValue] = <factory>) -> None'"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "WorkflowPreviewIn",
  "unit": "export"
}
```

</details>
