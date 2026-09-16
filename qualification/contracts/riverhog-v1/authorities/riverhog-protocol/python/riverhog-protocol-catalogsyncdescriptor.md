# riverhog_protocol.CatalogSyncDescriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-catalogsyncdescriptor:eab7a09e06 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-56ec8b3127"></a>
- <a id="s-85d3c30427"></a>`distribution`: `riverhog-protocol`
- <a id="s-3cc6fcb805"></a>`module`: `riverhog_protocol`
- <a id="s-d71acd22c8"></a>`name`: `CatalogSyncDescriptor`
- <a id="s-d5d674c0a1"></a>`unit`: `export`

### Declared structure

- <a id="s-60b8170405"></a>`kind`: `"class"`
- <a id="s-ee99b0db9a"></a>`signature`: `"\"(*, collection_id: CollectionId, archive_root_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], content_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], description: CollectionDescription \| None, description_revision: Annotated[int, Strict(strict=True), Ge(ge=0), Le(le=9007199254740991)], description_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], tag_revision: Annotated[int, Strict(strict=True), Ge(ge=1), Le(le=9007199254740991)], tag_set_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], revision: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=19, pattern='^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-6d0b0279c0"></a>

- <a id="s-36a77fb506"></a>`type`: `"object"`
- <a id="s-ccc76d83db"></a>`additionalProperties`: `false`
- <a id="s-23197de41b"></a>`required`: `["collection_id","archive_root_sha256","content_identity","description","description_revision","description_identity","tag_revision","tag_set_identity","revision"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2cad3114a7"></a>`archive_root_sha256` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-83b72884f7"></a>`collection_id` | yes | [CollectionId](#s-45d9f823fc) |  |
| <a id="s-8c83d805d7"></a>`content_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f896fc541c"></a>`description` | yes | anyOf=([CollectionDescription](#s-a0be6916ca)) \| (type="null") |  |
| <a id="s-d0a0d83002"></a>`description_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a75af70b7e"></a>`description_revision` | yes | type="integer"; minimum=0; maximum=9007199254740991 |  |
| <a id="s-de58e57e97"></a>`revision` | yes | type="string"; maxLength=19; minLength=1; pattern="^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$" |  |
| <a id="s-291e046f33"></a>`tag_revision` | yes | type="integer"; minimum=1; maximum=9007199254740991 |  |
| <a id="s-6584252cae"></a>`tag_set_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [CollectionDescription](#s-a0be6916ca)
- [CollectionId](#s-45d9f823fc)

##### <a id="s-a0be6916ca"></a>definition `CollectionDescription`

- <a id="s-94041b9f92"></a>`type`: `"string"`
- <a id="s-02e29cbc80"></a>`maxLength`: `32768`
- <a id="s-e4e502d1ec"></a>`minLength`: `1`
- <a id="s-c8a5a04cfa"></a>`x-riverhog-encoded-bytes-max`: `32768`
- <a id="s-c7c5b3fe19"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-catalog-description"}`
- <a id="s-96e8398251"></a>`x-unicode-normalization`: `"NFC"`

##### <a id="s-45d9f823fc"></a>definition `CollectionId`

- <a id="s-0f445ff36e"></a>`type`: `"integer"`
- <a id="s-a635191fd1"></a>`minimum`: `1`

## Governing policies

- <a id="pa-e0342cb6f9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CatalogSyncDescriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 56ac87b11d976aacacb42ca478f9f8328d1721ce3c268d6f829413353b72aeab -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
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
    "signature": "\"(*, collection_id: CollectionId, archive_root_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], content_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], description: CollectionDescription | None, description_revision: Annotated[int, Strict(strict=True), Ge(ge=0), Le(le=9007199254740991)], description_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], tag_revision: Annotated[int, Strict(strict=True), Ge(ge=1), Le(le=9007199254740991)], tag_set_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], revision: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=19, pattern='^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18})$', ascii_only=None)]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CatalogSyncDescriptor",
  "unit": "export"
}
```

</details>
