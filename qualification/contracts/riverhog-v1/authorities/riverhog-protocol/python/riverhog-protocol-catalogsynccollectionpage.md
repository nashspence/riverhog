# riverhog_protocol.CatalogSyncCollectionPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-catalogsynccollectionpage:b333e98a32 -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-d323fec92c"></a>`title`: CatalogSyncCollectionPage
- <a id="s-41ab9bc9c1"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-de5dee8417"></a>`authorization_view_identity` | yes | type="string"; minLength=64; maxLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-84ebedcdca"></a>`changes_cursor` | no | anyOf=type="string"; minLength=1; maxLength=4096 \| type="null" |  |
| <a id="s-b1b4fdf8cf"></a>`collections` | yes | type="array"; maxItems=100; items=(#/$defs/CatalogSyncDescriptor) |  |
| <a id="s-91d2f7e6a2"></a>`format` | no | type="string"; const="riverhog-catalog-sync/v1" |  |
| <a id="s-e4c6d56bef"></a>`next_cursor` | no | anyOf=type="string"; minLength=1; maxLength=4096 \| type="null" |  |
| <a id="s-84522265fb"></a>`source_identity` | yes | type="string"; minLength=64; maxLength=64; pattern="^[0-9a-f]{64}$" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-8c25c1131c"></a>`CatalogSyncDescriptor` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`, `description`, `description_identity`, `description_revision`, `revision`, `tag_revision`, `tag_set_identity`; additional keys=`additionalProperties`, `required` |
| <a id="s-9422def1b1"></a>`CollectionDescription` | type="string"; minLength=1; maxLength=32768; additional keys=`x-riverhog-encoded-bytes-max`, `x-riverhog-extent`, `x-unicode-normalization` |
| <a id="s-a83fcd9192"></a>`CollectionId` | type="integer"; minimum=1 |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CatalogSyncCollectionPage.validate_continuation](riverhog-protocol-catalogsynccollectionpage-validate-continuation.md)

## Governing policies

- <a id="pa-9ed2e7ae86"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CatalogSyncCollectionPage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 043cba73d68cb7c25e8c985d254283c3eeca5af7c45f4b5eb520227ef2f03902 -->

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
              "title": "Archive Root Sha256",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "content_identity": {
              "maxLength": 64,
              "minLength": 64,
              "pattern": "^[0-9a-f]{64}$",
              "title": "Content Identity",
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
              "title": "Description Identity",
              "type": "string"
            },
            "description_revision": {
              "maximum": 9007199254740991,
              "minimum": 0,
              "title": "Description Revision",
              "type": "integer"
            },
            "revision": {
              "maxLength": 19,
              "minLength": 1,
              "pattern": "^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18})$",
              "title": "Revision",
              "type": "string"
            },
            "tag_revision": {
              "maximum": 9007199254740991,
              "minimum": 1,
              "title": "Tag Revision",
              "type": "integer"
            },
            "tag_set_identity": {
              "maxLength": 64,
              "minLength": 64,
              "pattern": "^[0-9a-f]{64}$",
              "title": "Tag Set Identity",
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
          "title": "CatalogSyncDescriptor",
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
          "title": "Authorization View Identity",
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
          "default": null,
          "title": "Changes Cursor"
        },
        "collections": {
          "items": {
            "$ref": "#/$defs/CatalogSyncDescriptor"
          },
          "maxItems": 100,
          "title": "Collections",
          "type": "array"
        },
        "format": {
          "const": "riverhog-catalog-sync/v1",
          "default": "riverhog-catalog-sync/v1",
          "title": "Format",
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
          "default": null,
          "title": "Next Cursor"
        },
        "source_identity": {
          "maxLength": 64,
          "minLength": 64,
          "pattern": "^[0-9a-f]{64}$",
          "title": "Source Identity",
          "type": "string"
        }
      },
      "required": [
        "source_identity",
        "authorization_view_identity",
        "collections"
      ],
      "title": "CatalogSyncCollectionPage",
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
