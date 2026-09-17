# riverhog_protocol.CatalogSyncChangePage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-catalogsyncchangepage:dcea2a6ab3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-784433f1d0"></a>
- <a id="s-98056499f6"></a>`distribution`: `riverhog-protocol`
- <a id="s-e576aa7c08"></a>`module`: `riverhog_protocol`
- <a id="s-9a667e7ee4"></a>`name`: `CatalogSyncChangePage`
- <a id="s-03688897b7"></a>`unit`: `export`

### Declared structure

- <a id="s-01561c04e8"></a>`kind`: `"class"`
- <a id="s-09a60b0417"></a>`signature`: `"\"(*, format: Literal['riverhog-catalog-sync/v1'] = 'riverhog-catalog-sync/v1', source_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], authorization_view_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], changes: Annotated[list[Annotated[riverhog_protocol.catalog_sync.CatalogSyncUpsert \| riverhog_protocol.catalog_sync.CatalogSyncDelete, FieldInfo(annotation=NoneType, required=True, discriminator='operation')]], MaxLen(max_length=100)], next_cursor: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=4096, pattern=None, ascii_only=None)], caught_up: bool, through_revision: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=19, pattern='^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18})$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-41e3ad29a6"></a>

- <a id="s-af3d3bf176"></a>`type`: `"object"`
- <a id="s-4ba5954d79"></a>`additionalProperties`: `false`
- <a id="s-dd92c57493"></a>`required`: `["source_identity","authorization_view_identity","changes","next_cursor","caught_up","through_revision"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0a4585062d"></a>`authorization_view_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-41c8947328"></a>`caught_up` | yes | type="boolean" |  |
| <a id="s-c2575cc1c5"></a>`changes` | yes | type="array"; items=(discriminator={"mapping":{"delete":"#/$defs/CatalogSyncDelete","upsert":"#/$defs/CatalogSyncUpsert"},"propertyName":"operation"}; oneOf=[([CatalogSyncUpsert](#s-4015421cfa)); ([CatalogSyncDelete](#s-168dffbf4b))]); maxItems=100 |  |
| <a id="s-abca29af81"></a>`format` | no | type="string"; const="riverhog-catalog-sync/v1"; default="riverhog-catalog-sync/v1" |  |
| <a id="s-708ac3cf38"></a>`next_cursor` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-d0e9a7990f"></a>`source_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b4ec350e94"></a>`through_revision` | yes | type="string"; maxLength=19; minLength=1; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18})$" |  |

##### Definitions

- [CatalogSyncDelete](#s-168dffbf4b)
- [CatalogSyncUpsert](#s-4015421cfa)
- [CollectionDescription](#s-aafc0c1ae8)
- [CollectionId](#s-389380939c)

##### <a id="s-168dffbf4b"></a>definition `CatalogSyncDelete`

- <a id="s-668c57e50d"></a>`type`: `"object"`
- <a id="s-36a38667f6"></a>`additionalProperties`: `false`
- <a id="s-5ee264b16b"></a>`required`: `["collection_id","revision"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d6acbd562e"></a>`collection_id` | yes | [CollectionId](#s-389380939c) |  |
| <a id="s-0f2bac5ce5"></a>`operation` | no | type="string"; const="delete"; default="delete" |  |
| <a id="s-45aa2629f1"></a>`revision` | yes | type="string"; maxLength=19; minLength=1; pattern="^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$" |  |

##### <a id="s-4015421cfa"></a>definition `CatalogSyncUpsert`

- <a id="s-650825daaa"></a>`type`: `"object"`
- <a id="s-c35c20cbf0"></a>`additionalProperties`: `false`
- <a id="s-2b8912e396"></a>`required`: `["collection_id","archive_root_sha256","content_identity","description","description_revision","description_identity","tag_revision","tag_set_identity","revision"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-538e027e62"></a>`archive_root_sha256` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1b69b1a43a"></a>`collection_id` | yes | [CollectionId](#s-389380939c) |  |
| <a id="s-f358322e30"></a>`content_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-77c3b2ee5e"></a>`description` | yes | anyOf=[([CollectionDescription](#s-aafc0c1ae8)); (type="null")] |  |
| <a id="s-9be1b15cf2"></a>`description_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b13e8e62b6"></a>`description_revision` | yes | type="integer"; minimum=0; maximum=9007199254740991 |  |
| <a id="s-01ffef1667"></a>`operation` | no | type="string"; const="upsert"; default="upsert" |  |
| <a id="s-292c7f971c"></a>`revision` | yes | type="string"; maxLength=19; minLength=1; pattern="^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$" |  |
| <a id="s-56c9249cab"></a>`tag_revision` | yes | type="integer"; minimum=1; maximum=9007199254740991 |  |
| <a id="s-730a5bad66"></a>`tag_set_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-aafc0c1ae8"></a>definition `CollectionDescription`

- <a id="s-e522f5fd76"></a>`type`: `"string"`
- <a id="s-76fa736420"></a>`maxLength`: `32768`
- <a id="s-1f4fde7293"></a>`minLength`: `1`
- <a id="s-a2dcdc4ab8"></a>`x-riverhog-encoded-bytes-max`: `32768`
- <a id="s-decf001d0c"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-catalog-description"}`
- <a id="s-469133de35"></a>`x-unicode-normalization`: `"NFC"`

##### <a id="s-389380939c"></a>definition `CollectionId`

- <a id="s-9dddada050"></a>`type`: `"integer"`
- <a id="s-669cfd00a3"></a>`minimum`: `1`

## Governing policies

- <a id="pa-49beda2f83"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CatalogSyncChangePage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 963b6552744388a2c22a5dac28688748dc2df89a9ca44324f6d3d307b7779601 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CatalogSyncDelete": {
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
        "CatalogSyncUpsert": {
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
            "operation": {
              "const": "upsert",
              "default": "upsert",
              "type": "string"
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
        "caught_up": {
          "type": "boolean"
        },
        "changes": {
          "items": {
            "discriminator": {
              "mapping": {
                "delete": "#/$defs/CatalogSyncDelete",
                "upsert": "#/$defs/CatalogSyncUpsert"
              },
              "propertyName": "operation"
            },
            "oneOf": [
              {
                "$ref": "#/$defs/CatalogSyncUpsert"
              },
              {
                "$ref": "#/$defs/CatalogSyncDelete"
              }
            ]
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
          "maxLength": 4096,
          "minLength": 1,
          "type": "string"
        },
        "source_identity": {
          "maxLength": 64,
          "minLength": 64,
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "through_revision": {
          "maxLength": 19,
          "minLength": 1,
          "pattern": "^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18})$",
          "type": "string"
        }
      },
      "required": [
        "source_identity",
        "authorization_view_identity",
        "changes",
        "next_cursor",
        "caught_up",
        "through_revision"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['riverhog-catalog-sync/v1'] = 'riverhog-catalog-sync/v1', source_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], authorization_view_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], changes: Annotated[list[Annotated[riverhog_protocol.catalog_sync.CatalogSyncUpsert | riverhog_protocol.catalog_sync.CatalogSyncDelete, FieldInfo(annotation=NoneType, required=True, discriminator='operation')]], MaxLen(max_length=100)], next_cursor: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=4096, pattern=None, ascii_only=None)], caught_up: bool, through_revision: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=19, pattern='^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18})$', ascii_only=None)]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CatalogSyncChangePage",
  "unit": "export"
}
```

</details>
