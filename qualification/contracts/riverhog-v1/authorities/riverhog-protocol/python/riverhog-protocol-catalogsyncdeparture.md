# riverhog_protocol.CatalogSyncDeparture

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-catalogsyncdeparture:341906736c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cc16e7e75e"></a>
- <a id="s-1cab4f14b4"></a>`distribution`: `riverhog-protocol`
- <a id="s-67c2eafb82"></a>`module`: `riverhog_protocol`
- <a id="s-8f56bad22b"></a>`name`: `CatalogSyncDeparture`
- <a id="s-f423e7062e"></a>`unit`: `export`

### Declared structure

- <a id="s-44a6b0547d"></a>`kind`: `"class"`
- <a id="s-530ef0a8c3"></a>`signature`: `"\"(*, operation: Literal['departure'] = 'departure', cause: Literal['collection_deleted', 'visibility_lost'], collection_id: CollectionId, revision: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=19, pattern='^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-9a0f2b3421"></a>

- <a id="s-edd1454af3"></a>`type`: `"object"`
- <a id="s-6ba0d21ef5"></a>`additionalProperties`: `false`
- <a id="s-3978ce11a7"></a>`required`: `["cause","collection_id","revision"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-096c9ebe94"></a>`cause` | yes | type="string"; enum=["collection_deleted","visibility_lost"] |  |
| <a id="s-2e07e34088"></a>`collection_id` | yes | [CollectionId](#s-b67313cd03) |  |
| <a id="s-9686fb82f1"></a>`operation` | no | type="string"; const="departure"; default="departure" |  |
| <a id="s-039c6803b6"></a>`revision` | yes | type="string"; maxLength=19; minLength=1; pattern="^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$" |  |

##### Definitions

- [CollectionId](#s-b67313cd03)

##### <a id="s-b67313cd03"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-0454319a29"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-48a62254f0"></a>2 | not=(const="0") |

## Governing policies

- <a id="pa-9256d9b687"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CatalogSyncDeparture`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 23e3b92506f61f7669efc4acd6b7b3e5b8fad7f2beaf02d458f2b7f7323b638b -->

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
        "cause": {
          "enum": [
            "collection_deleted",
            "visibility_lost"
          ],
          "type": "string"
        },
        "collection_id": {
          "$ref": "#/$defs/CollectionId"
        },
        "operation": {
          "const": "departure",
          "default": "departure",
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
        "cause",
        "collection_id",
        "revision"
      ],
      "type": "object"
    },
    "signature": "\"(*, operation: Literal['departure'] = 'departure', cause: Literal['collection_deleted', 'visibility_lost'], collection_id: CollectionId, revision: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=19, pattern='^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18})$', ascii_only=None)]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CatalogSyncDeparture",
  "unit": "export"
}
```

</details>
