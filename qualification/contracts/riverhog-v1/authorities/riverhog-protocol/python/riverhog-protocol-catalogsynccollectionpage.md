# riverhog_protocol.CatalogSyncCollectionPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-catalogsynccollectionpage:b333e98a32 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-670992d616"></a>
- <a id="s-687dba8f0b"></a>`distribution`: `riverhog-protocol`
- <a id="s-786f21b3dc"></a>`module`: `riverhog_protocol`
- <a id="s-f611e36554"></a>`name`: `CatalogSyncCollectionPage`
- <a id="s-226d2c8593"></a>`unit`: `export`

### Declared structure

- <a id="s-f5fe250bbd"></a>`kind`: `"class"`
- <a id="s-12afa411fa"></a>`signature`: `"\"(*, format: Literal['riverhog-catalog-sync/v1'] = 'riverhog-catalog-sync/v1', source_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], authorization_view_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], collections: Annotated[list[riverhog_protocol.catalog_sync.CatalogSyncDescriptor], MaxLen(max_length=100)], next_cursor: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=4096, pattern=None, ascii_only=None)]] = None, changes_cursor: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=4096, pattern=None, ascii_only=None)]] = None) -> None\""`

#### Validated model schema

<a id="s-d4ba1e9ed8"></a>

- <a id="s-41ab9bc9c1"></a>`type`: `"object"`
- <a id="s-b8389d738b"></a>`additionalProperties`: `false`
- <a id="s-8be0b79946"></a>`required`: `["source_identity","authorization_view_identity","collections"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-de5dee8417"></a>`authorization_view_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-84ebedcdca"></a>`changes_cursor` | no | anyOf=[(type="string"; maxLength=4096; minLength=1); (type="null")]; default=null |  |
| <a id="s-b1b4fdf8cf"></a>`collections` | yes | type="array"; items=([CatalogSyncDescriptor](#s-8c25c1131c)); maxItems=100 |  |
| <a id="s-91d2f7e6a2"></a>`format` | no | type="string"; const="riverhog-catalog-sync/v1"; default="riverhog-catalog-sync/v1" |  |
| <a id="s-e4c6d56bef"></a>`next_cursor` | no | anyOf=[(type="string"; maxLength=4096; minLength=1); (type="null")]; default=null |  |
| <a id="s-84522265fb"></a>`source_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [CatalogSyncDescriptor](#s-8c25c1131c)
- [CollectionDescription](#s-9422def1b1)
- [CollectionId](#s-a83fcd9192)

##### <a id="s-8c25c1131c"></a>definition `CatalogSyncDescriptor`

- <a id="s-88b763a076"></a>`type`: `"object"`
- <a id="s-38775328d1"></a>`additionalProperties`: `false`
- <a id="s-1efd2d8931"></a>`required`: `["collection_id","archive_root_sha256","content_identity","description","description_revision","description_identity","tag_revision","tag_set_identity","revision"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2a22a4e16b"></a>`archive_root_sha256` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6418b0cd40"></a>`collection_id` | yes | [CollectionId](#s-a83fcd9192) |  |
| <a id="s-7255ac678a"></a>`content_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2b03d9b4fd"></a>`description` | yes | anyOf=[([CollectionDescription](#s-9422def1b1)); (type="null")] |  |
| <a id="s-5e814898de"></a>`description_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-dd33ef622b"></a>`description_revision` | yes | type="integer"; minimum=0; maximum=9007199254740991 |  |
| <a id="s-641ef1956d"></a>`revision` | yes | type="string"; maxLength=19; minLength=1; pattern="^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$" |  |
| <a id="s-cde77198b5"></a>`tag_revision` | yes | type="integer"; minimum=1; maximum=9007199254740991 |  |
| <a id="s-25715b5c06"></a>`tag_set_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-9422def1b1"></a>definition `CollectionDescription`

- <a id="s-93af252e07"></a>`type`: `"string"`
- <a id="s-54613cb947"></a>`maxLength`: `32768`
- <a id="s-b574be85fd"></a>`minLength`: `1`
- <a id="s-44dea2b09d"></a>`x-riverhog-encoded-bytes-max`: `32768`
- <a id="s-f4f165d53d"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-catalog-description"}`
- <a id="s-5f5dd78faa"></a>`x-unicode-normalization`: `"NFC"`

##### <a id="s-a83fcd9192"></a>definition `CollectionId`

- <a id="s-8746180cec"></a>`type`: `"integer"`
- <a id="s-1753f8624a"></a>`minimum`: `1`

## Maintained corroboration

### Related interface records

- [validate_continuation](riverhog-protocol-catalogsynccollectionpage-validate-continuation.md)

## Governing policies

- <a id="pa-9ed2e7ae86"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CatalogSyncCollectionPage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ca946cbd63e78d60262f1f63879f7e855459ccb8a092d20f42eda9cc6427ae6f -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CatalogSyncDescriptor": {
          "additionalProperties": false,
          "properties": {
            "archive_root_sha256": {
              "maxLength": 64,
              "minLength": 64,
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "content_identity": {
              "maxLength": 64,
              "minLength": 64,
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "description": {
              "anyOf": [
                {
                  "$ref": "#/$defs/CollectionDescription"
                },
                {
                  "type": "null"
                }
              ]
            },
            "description_identity": {
              "maxLength": 64,
              "minLength": 64,
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "description_revision": {
              "maximum": 9007199254740991,
              "minimum": 0,
              "type": "integer"
            },
            "revision": {
              "maxLength": 19,
              "minLength": 1,
              "pattern": "^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18})$",
              "type": "string"
            },
            "tag_revision": {
              "maximum": 9007199254740991,
              "minimum": 1,
              "type": "integer"
            },
            "tag_set_identity": {
              "maxLength": 64,
              "minLength": 64,
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "collection_id",
            "archive_root_sha256",
            "content_identity",
            "description",
            "description_revision",
            "description_identity",
            "tag_revision",
            "tag_set_identity",
            "revision"
          ],
          "type": "object"
        },
        "CollectionDescription": {
          "maxLength": 32768,
          "minLength": 1,
          "type": "string",
          "x-riverhog-encoded-bytes-max": 32768,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-human-authored-catalog-description"
          },
          "x-unicode-normalization": "NFC"
        },
        "CollectionId": {
          "minimum": 1,
          "type": "integer"
        }
      },
      "additionalProperties": false,
      "properties": {
        "authorization_view_identity": {
          "maxLength": 64,
          "minLength": 64,
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "changes_cursor": {
          "anyOf": [
            {
              "maxLength": 4096,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "collections": {
          "items": {
            "$ref": "#/$defs/CatalogSyncDescriptor"
          },
          "maxItems": 100,
          "type": "array"
        },
        "format": {
          "const": "riverhog-catalog-sync/v1",
          "default": "riverhog-catalog-sync/v1",
          "type": "string"
        },
        "next_cursor": {
          "anyOf": [
            {
              "maxLength": 4096,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "source_identity": {
          "maxLength": 64,
          "minLength": 64,
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "source_identity",
        "authorization_view_identity",
        "collections"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['riverhog-catalog-sync/v1'] = 'riverhog-catalog-sync/v1', source_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], authorization_view_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], collections: Annotated[list[riverhog_protocol.catalog_sync.CatalogSyncDescriptor], MaxLen(max_length=100)], next_cursor: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=4096, pattern=None, ascii_only=None)]] = None, changes_cursor: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=4096, pattern=None, ascii_only=None)]] = None) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CatalogSyncCollectionPage",
  "unit": "export"
}
```

</details>
