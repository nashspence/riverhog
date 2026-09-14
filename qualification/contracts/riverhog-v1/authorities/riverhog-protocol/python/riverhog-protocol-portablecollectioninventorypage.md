# riverhog_protocol.PortableCollectionInventoryPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-portablecollectioninventorypage:180fca9afb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6a35447804"></a>
- <a id="s-802a817536"></a>`distribution`: `riverhog-protocol`
- <a id="s-4052838d48"></a>`module`: `riverhog_protocol`
- <a id="s-1e00a5d1ef"></a>`name`: `PortableCollectionInventoryPage`
- <a id="s-2d41295a95"></a>`unit`: `export`

### Declared structure

- <a id="s-bb5a8e4821"></a>`kind`: `"class"`
- <a id="s-32ad91debc"></a>`signature`: `"\"(*, format: Literal['riverhog-collection-inventory-page/v1'] = 'riverhog-collection-inventory-page/v1', authority: riverhog_protocol.portable_collection.PortableCollectionInventoryAuthority, files: Annotated[list[riverhog_protocol.file_identity.ImmutableFileIdentityDocument], MaxLen(max_length=1000)], next_cursor: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=8192)] = None, complete: bool) -> None\""`

#### Validated model schema

<a id="s-d7dbb6340d"></a>
- <a id="s-84bdcafeb0"></a>`title`: PortableCollectionInventoryPage
- <a id="s-37913469f0"></a>`description`: One bounded, canonically ordered slice of an immutable inventory.
- <a id="s-6f91f83dce"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5d0beadb56"></a>`authority` | yes | #/$defs/PortableCollectionInventoryAuthority |  |
| <a id="s-f5a81c0a53"></a>`complete` | yes | type="boolean" |  |
| <a id="s-32163b904e"></a>`files` | yes | type="array"; maxItems=1000; items=(#/$defs/ImmutableFileIdentityDocument); additional keys=`x-riverhog-extent` |  |
| <a id="s-daf906b4ae"></a>`format` | no | type="string"; const="riverhog-collection-inventory-page/v1" |  |
| <a id="s-a8c96c4171"></a>`next_cursor` | no | anyOf=type="string"; minLength=1; maxLength=8192 \| type="null" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-859b45e2c0"></a>`CanonicalRelPath` | type="string"; format="riverhog-canonical-relpath-v1"; minLength=1; maxLength=4096; pattern="^[^/\\\\]+(?:/[^/\\\\]+)*$"; allOf=additional keys=`not` \| additional keys=`not`; additional keys=`x-unicode-normalization` |
| <a id="s-75898504ca"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-9185aadd43"></a>`ImmutableFileIdentityDocument` | type="object"; fields=`bytes`, `path`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-c2615d6cd5"></a>`PortableCollectionHeader` | type="object"; fields=`collection`, `content_identity`, `encryption_format`, `format`, `passphrase_id`, `provenance_identity`, `provenance_mode`; additional keys=`additionalProperties`, `required` |
| <a id="s-58d933375e"></a>`PortableCollectionInventoryAuthority` | type="object"; fields=`file_bytes`, `file_count`, `header`, `inventory_identity`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.PortableCollectionInventoryPage.validate_page](riverhog-protocol-portablecollectioninventorypage-validate-page.md)

## Governing policies

- <a id="pa-3e89fa838e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.PortableCollectionInventoryPage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dddf74e2fa3824121b299890929fd2fd1d2edd7f226844878d02aab9716124d0 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CanonicalRelPath": {
          "allOf": [
            {
              "not": {
                "pattern": "(?:^|/)\\.{1,2}(?:/|$)"
              }
            },
            {
              "not": {
                "pattern": "^\\s|\\s$"
              }
            }
          ],
          "format": "riverhog-canonical-relpath-v1",
          "maxLength": 4096,
          "minLength": 1,
          "pattern": "^[^/\\\\]+(?:/[^/\\\\]+)*$",
          "type": "string",
          "x-unicode-normalization": "NFC"
        },
        "CollectionId": {
          "minimum": 1,
          "type": "integer"
        },
        "ImmutableFileIdentityDocument": {
          "additionalProperties": false,
          "description": "The exact path, length, and plaintext digest shared by file projections.",
          "properties": {
            "bytes": {
              "minimum": 0,
              "title": "Bytes",
              "type": "integer"
            },
            "path": {
              "$ref": "#/$defs/CanonicalRelPath"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Sha256",
              "type": "string"
            }
          },
          "required": [
            "path",
            "bytes",
            "sha256"
          ],
          "title": "ImmutableFileIdentityDocument",
          "type": "object"
        },
        "PortableCollectionHeader": {
          "additionalProperties": false,
          "description": "Bounded immutable metadata that owns one portable file inventory.",
          "properties": {
            "collection": {
              "$ref": "#/$defs/CollectionId"
            },
            "content_identity": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Content Identity",
              "type": "string"
            },
            "encryption_format": {
              "minLength": 1,
              "title": "Encryption Format",
              "type": "string"
            },
            "format": {
              "const": "riverhog-collection/v1",
              "default": "riverhog-collection/v1",
              "title": "Format",
              "type": "string"
            },
            "passphrase_id": {
              "pattern": "^[A-Za-z0-9_-]{16,128}$",
              "title": "Passphrase Id",
              "type": "string"
            },
            "provenance_identity": {
              "anyOf": [
                {
                  "pattern": "^[0-9a-f]{64}$",
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Provenance Identity"
            },
            "provenance_mode": {
              "enum": [
                "captured",
                "mixed",
                "omitted"
              ],
              "title": "Provenance Mode",
              "type": "string"
            }
          },
          "required": [
            "collection",
            "content_identity",
            "encryption_format",
            "passphrase_id",
            "provenance_mode"
          ],
          "title": "PortableCollectionHeader",
          "type": "object"
        },
        "PortableCollectionInventoryAuthority": {
          "additionalProperties": false,
          "description": "The immutable authority shared by every bounded inventory page.",
          "properties": {
            "file_bytes": {
              "minimum": 0,
              "title": "File Bytes",
              "type": "integer"
            },
            "file_count": {
              "minimum": 1,
              "title": "File Count",
              "type": "integer"
            },
            "header": {
              "$ref": "#/$defs/PortableCollectionHeader"
            },
            "inventory_identity": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Inventory Identity",
              "type": "string"
            }
          },
          "required": [
            "header",
            "inventory_identity",
            "file_count",
            "file_bytes"
          ],
          "title": "PortableCollectionInventoryAuthority",
          "type": "object"
        }
      },
      "additionalProperties": false,
      "description": "One bounded, canonically ordered slice of an immutable inventory.",
      "properties": {
        "authority": {
          "$ref": "#/$defs/PortableCollectionInventoryAuthority"
        },
        "complete": {
          "title": "Complete",
          "type": "boolean"
        },
        "files": {
          "items": {
            "$ref": "#/$defs/ImmutableFileIdentityDocument"
          },
          "maxItems": 1000,
          "title": "Files",
          "type": "array",
          "x-riverhog-extent": {
            "policy": "segmented_no_total_max",
            "progression": "authority-bound-cursor",
            "reason": "bounded-portable-inventory-page"
          }
        },
        "format": {
          "const": "riverhog-collection-inventory-page/v1",
          "default": "riverhog-collection-inventory-page/v1",
          "title": "Format",
          "type": "string"
        },
        "next_cursor": {
          "anyOf": [
            {
              "maxLength": 8192,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Next Cursor"
        }
      },
      "required": [
        "authority",
        "files",
        "complete"
      ],
      "title": "PortableCollectionInventoryPage",
      "type": "object"
    },
    "signature": "\"(*, format: Literal['riverhog-collection-inventory-page/v1'] = 'riverhog-collection-inventory-page/v1', authority: riverhog_protocol.portable_collection.PortableCollectionInventoryAuthority, files: Annotated[list[riverhog_protocol.file_identity.ImmutableFileIdentityDocument], MaxLen(max_length=1000)], next_cursor: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=8192)] = None, complete: bool) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "PortableCollectionInventoryPage",
  "unit": "export"
}
```
