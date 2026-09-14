# riverhog_protocol.CatalogSyncDelete

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-catalogsyncdelete:b86be775c8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bf6aadfc13"></a>
- <a id="s-ccbef6b4ec"></a>`distribution`: `riverhog-protocol`
- <a id="s-d866eb54b7"></a>`module`: `riverhog_protocol`
- <a id="s-0a9d4140f8"></a>`name`: `CatalogSyncDelete`
- <a id="s-86b99a8281"></a>`unit`: `export`

### Declared structure

- <a id="s-b839f8b8b8"></a>`kind`: `"class"`
- <a id="s-d52850d684"></a>`signature`: `"\"(*, operation: Literal['delete'] = 'delete', collection_id: CollectionId, revision: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=19, pattern='^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-79de46c8fb"></a>
- <a id="s-ae669fe636"></a>`title`: CatalogSyncDelete
- <a id="s-2a14510f0c"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e49e26afec"></a>`collection_id` | yes | #/$defs/CollectionId |  |
| <a id="s-158b4ec8c7"></a>`operation` | no | type="string"; const="delete" |  |
| <a id="s-1a3a7c99bf"></a>`revision` | yes | type="string"; minLength=1; maxLength=19; pattern="^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-de7607d230"></a>`CollectionId` | type="integer"; minimum=1 |

## Governing policies

- <a id="pa-d3223a6e95"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CatalogSyncDelete`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 872bc9c9c399500bd4d1d17900b48eb31a5d44130cfff6664735d752602dc50d -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CollectionId": {
          "minimum": 1,
          "type": "integer"
        }
      },
      "additionalProperties": false,
      "properties": {
        "collection_id": {
          "$ref": "#/$defs/CollectionId"
        },
        "operation": {
          "const": "delete",
          "default": "delete",
          "title": "Operation",
          "type": "string"
        },
        "revision": {
          "maxLength": 19,
          "minLength": 1,
          "pattern": "^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18})$",
          "title": "Revision",
          "type": "string"
        }
      },
      "required": [
        "collection_id",
        "revision"
      ],
      "title": "CatalogSyncDelete",
      "type": "object"
    },
    "signature": "\"(*, operation: Literal['delete'] = 'delete', collection_id: CollectionId, revision: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=19, pattern='^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18})$', ascii_only=None)]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CatalogSyncDelete",
  "unit": "export"
}
```
