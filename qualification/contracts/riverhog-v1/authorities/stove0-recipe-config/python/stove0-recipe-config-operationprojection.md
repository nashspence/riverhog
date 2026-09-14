# stove0_recipe_config.OperationProjection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-operationprojection:bb108842e3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-61dd36bc6c"></a>
- <a id="s-4648f6e419"></a>`distribution`: `stove0-recipe-config`
- <a id="s-3918a9b4ec"></a>`module`: `stove0_recipe_config`
- <a id="s-c8dbc8e458"></a>`name`: `OperationProjection`
- <a id="s-b5754eacc4"></a>`unit`: `export`

### Declared structure

- <a id="s-30d36926ed"></a>`kind`: `"class"`
- <a id="s-743be8796f"></a>`signature`: `"\"(*, source: Literal['work-effective-intent', 'work-evaluation'], source_pointer: Annotated[str, _PydanticGeneralMetadata(pattern='^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$')], destination: Literal['intent', 'target-options'], destination_pointer: Annotated[str, _PydanticGeneralMetadata(pattern='^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$')]) -> None\""`

#### Validated model schema

<a id="s-5c52b4089d"></a>
- <a id="s-5ecc719c8e"></a>`title`: OperationProjection
- <a id="s-a30b86f268"></a>`description`: One declarative JSON-pointer copy into an operation request.
- <a id="s-786b75a7f2"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8d01d2bdae"></a>`destination` | yes | type="string"; enum=["intent","target-options"] |  |
| <a id="s-4f14b8918b"></a>`destination_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-86d965e028"></a>`source` | yes | type="string"; enum=["work-effective-intent","work-evaluation"] |  |
| <a id="s-0d47256377"></a>`source_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |

## Governing policies

- <a id="pa-5d0a0cc13c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/stove0_recipe_config.OperationProjection`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 31be4abad0ddc9c6fe983b7166e37664b6c1c1e41a4a0e90484075400e7b669f -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "description": "One declarative JSON-pointer copy into an operation request.",
      "properties": {
        "destination": {
          "enum": [
            "intent",
            "target-options"
          ],
          "title": "Destination",
          "type": "string"
        },
        "destination_pointer": {
          "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
          "title": "Destination Pointer",
          "type": "string"
        },
        "source": {
          "enum": [
            "work-effective-intent",
            "work-evaluation"
          ],
          "title": "Source",
          "type": "string"
        },
        "source_pointer": {
          "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
          "title": "Source Pointer",
          "type": "string"
        }
      },
      "required": [
        "source",
        "source_pointer",
        "destination",
        "destination_pointer"
      ],
      "title": "OperationProjection",
      "type": "object"
    },
    "signature": "\"(*, source: Literal['work-effective-intent', 'work-evaluation'], source_pointer: Annotated[str, _PydanticGeneralMetadata(pattern='^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$')], destination: Literal['intent', 'target-options'], destination_pointer: Annotated[str, _PydanticGeneralMetadata(pattern='^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$')]) -> None\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "OperationProjection",
  "unit": "export"
}
```
