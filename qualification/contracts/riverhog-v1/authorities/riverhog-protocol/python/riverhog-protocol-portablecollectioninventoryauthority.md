# riverhog_protocol.PortableCollectionInventoryAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-portablecollectioninven-cb5712bb2b:c7a58f0a5c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d63ed8a4d1"></a>
- <a id="s-0639af0917"></a>`distribution`: `riverhog-protocol`
- <a id="s-7722dce441"></a>`module`: `riverhog_protocol`
- <a id="s-b58fa2119e"></a>`name`: `PortableCollectionInventoryAuthority`
- <a id="s-1f229d4eb9"></a>`unit`: `export`

### Declared structure

- <a id="s-d31b14aa8a"></a>`kind`: `"class"`
- <a id="s-556cbcc9d9"></a>`signature`: `"\"(*, header: riverhog_protocol.portable_collection.PortableCollectionHeader, inventory_identity: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], file_count: Annotated[int, Ge(ge=1)], file_bytes: Annotated[int, Ge(ge=0)]) -> None\""`

#### Validated model schema

<a id="s-3faf3f53b8"></a>
- <a id="s-2a6f0f0cd5"></a>`title`: PortableCollectionInventoryAuthority
- <a id="s-97bd6d81f8"></a>`description`: The immutable authority shared by every bounded inventory page.
- <a id="s-6068f9ef62"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5a27b4f2b8"></a>`file_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-40606344be"></a>`file_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-42cbdd55a0"></a>`header` | yes | #/$defs/PortableCollectionHeader |  |
| <a id="s-925b6902ff"></a>`inventory_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-d363bae8a7"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-98fff72407"></a>`PortableCollectionHeader` | type="object"; fields=`collection`, `content_identity`, `encryption_format`, `format`, `passphrase_id`, `provenance_identity`, `provenance_mode`; additional keys=`additionalProperties`, `required` |

## Governing policies

- <a id="pa-895b4321e4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.PortableCollectionInventoryAuthority`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eee16cd1c47654c43434b2738368fc0f541fa8bc669328d98ff6b8fb6637f1be -->

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
        }
      },
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
    },
    "signature": "\"(*, header: riverhog_protocol.portable_collection.PortableCollectionHeader, inventory_identity: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], file_count: Annotated[int, Ge(ge=1)], file_bytes: Annotated[int, Ge(ge=0)]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "PortableCollectionInventoryAuthority",
  "unit": "export"
}
```
