# riverhog_protocol.PortableCollectionHeader

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-portablecollectionheader:a4e4fc7e72 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9b00c2ff9f"></a>
- <a id="s-b87c803e8a"></a>`distribution`: `riverhog-protocol`
- <a id="s-6d782a0492"></a>`module`: `riverhog_protocol`
- <a id="s-cbd8df3d12"></a>`name`: `PortableCollectionHeader`
- <a id="s-72fd8c0e01"></a>`unit`: `export`

### Declared structure

- <a id="s-4e46354c7a"></a>`kind`: `"class"`
- <a id="s-1e0473133f"></a>`signature`: `"\"(*, format: Literal['riverhog-collection/v1'] = 'riverhog-collection/v1', collection: CollectionId, content_identity: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], encryption_format: Annotated[str, MinLen(min_length=1)], passphrase_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9_-]{16,128}$')], provenance_mode: Literal['captured', 'mixed', 'omitted'], provenance_identity: Annotated[str \| None, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')] = None) -> None\""`

#### Validated model schema

<a id="s-bab12140ce"></a>
- <a id="s-15400d7e49"></a>`title`: PortableCollectionHeader
- <a id="s-11a83c3356"></a>`description`: Bounded immutable metadata that owns one portable file inventory.
- <a id="s-563ffec762"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-25bffff653"></a>`collection` | yes | #/$defs/CollectionId |  |
| <a id="s-bc8d4e6d1c"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0b502a3262"></a>`encryption_format` | yes | type="string"; minLength=1 |  |
| <a id="s-b5c657def0"></a>`format` | no | type="string"; const="riverhog-collection/v1" |  |
| <a id="s-00bf3b7928"></a>`passphrase_id` | yes | type="string"; pattern="^[A-Za-z0-9_-]{16,128}$" |  |
| <a id="s-017934e63a"></a>`provenance_identity` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-23cf53cbef"></a>`provenance_mode` | yes | type="string"; enum=["captured","mixed","omitted"] |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-739396f78e"></a>`CollectionId` | type="integer"; minimum=1 |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.PortableCollectionHeader.validate_provenance_binding](riverhog-protocol-portablecollectionheader-validate-provenance-binding.md)

## Governing policies

- <a id="pa-281326bbe1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.PortableCollectionHeader`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 05e65310ae8861a8788c32a24bab6aeef580afd0acec77d26489583c5f53593f -->

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
    "signature": "\"(*, format: Literal['riverhog-collection/v1'] = 'riverhog-collection/v1', collection: CollectionId, content_identity: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], encryption_format: Annotated[str, MinLen(min_length=1)], passphrase_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9_-]{16,128}$')], provenance_mode: Literal['captured', 'mixed', 'omitted'], provenance_identity: Annotated[str | None, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')] = None) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "PortableCollectionHeader",
  "unit": "export"
}
```
