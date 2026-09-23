# riverhog_protocol.CatalogSyncDelete

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-catalogsyncdelete:b86be775c8 -->

Exact externally visible contract owned by this contract element.

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

- <a id="s-2a14510f0c"></a>`type`: `"object"`
- <a id="s-24f5a3fbe1"></a>`additionalProperties`: `false`
- <a id="s-934dbcc801"></a>`required`: `["collection_id","revision"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e49e26afec"></a>`collection_id` | yes | [CollectionId](#s-de7607d230) |  |
| <a id="s-158b4ec8c7"></a>`operation` | no | type="string"; const="delete"; default="delete" |  |
| <a id="s-1a3a7c99bf"></a>`revision` | yes | type="string"; maxLength=19; minLength=1; pattern="^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$" |  |

##### Definitions

- [CollectionId](#s-de7607d230)

##### <a id="s-de7607d230"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-8e61500bb3"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-98a1fb9b8c"></a>2 | not=(const="0") |

## Governing policies

- <a id="pa-d3223a6e95"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CatalogSyncDelete`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 13b741f9787ee5b0e9b3398f95b893e2008c51989e3a1dd9698dae0ab662773e -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CollectionId": {
          "allOf": [
            {
              "pattern": "^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[0-1][0-9]{17}|92[0-1][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[0-1][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6][0-9]{0}|9223372036854775807)(?![\\s\\S])",
              "type": "string"
            },
            {
              "not": {
                "const": "0"
              }
            }
          ]
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
          "type": "string"
        },
        "revision": {
          "maxLength": 19,
          "minLength": 1,
          "pattern": "^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18})$",
          "type": "string"
        }
      },
      "required": [
        "collection_id",
        "revision"
      ],
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

</details>
