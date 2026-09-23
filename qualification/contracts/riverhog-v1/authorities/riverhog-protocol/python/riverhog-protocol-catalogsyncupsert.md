# riverhog_protocol.CatalogSyncUpsert

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-catalogsyncupsert:1873de7536 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8bfe62c4fa"></a>
- <a id="s-daf485e9af"></a>`distribution`: `riverhog-protocol`
- <a id="s-e3a4ebd778"></a>`module`: `riverhog_protocol`
- <a id="s-d67eebe5c4"></a>`name`: `CatalogSyncUpsert`
- <a id="s-07c0932b3a"></a>`unit`: `export`

### Declared structure

- <a id="s-4978b40f75"></a>`kind`: `"class"`
- <a id="s-d291987a70"></a>`signature`: `"\"(*, collection_id: CollectionId, archive_root_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], content_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], description: CollectionDescription \| None, description_revision: Annotated[int, Strict(strict=True), Ge(ge=0), Le(le=9007199254740991)], description_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], tag_revision: Annotated[int, Strict(strict=True), Ge(ge=1), Le(le=9007199254740991)], tag_set_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], revision: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=19, pattern='^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$', ascii_only=None)], operation: Literal['upsert'] = 'upsert') -> None\""`

#### Validated model schema

<a id="s-1fc0874d41"></a>

- <a id="s-4259efb4da"></a>`type`: `"object"`
- <a id="s-73eef63e6e"></a>`additionalProperties`: `false`
- <a id="s-9bfed41a65"></a>`required`: `["collection_id","archive_root_sha256","content_identity","description","description_revision","description_identity","tag_revision","tag_set_identity","revision"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2ccfa8abaa"></a>`archive_root_sha256` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-cdb7d0178e"></a>`collection_id` | yes | [CollectionId](#s-99e4ae8314) |  |
| <a id="s-93685e44c6"></a>`content_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-cb158b751e"></a>`description` | yes | anyOf=[([CollectionDescription](#s-95ff3511b7)); (type="null")] |  |
| <a id="s-2de72a865a"></a>`description_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-53fc15196d"></a>`description_revision` | yes | type="integer"; minimum=0; maximum=9007199254740991 |  |
| <a id="s-054311f13c"></a>`operation` | no | type="string"; const="upsert"; default="upsert" |  |
| <a id="s-09a5e95bd3"></a>`revision` | yes | type="string"; maxLength=19; minLength=1; pattern="^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$" |  |
| <a id="s-95f1646936"></a>`tag_revision` | yes | type="integer"; minimum=1; maximum=9007199254740991 |  |
| <a id="s-7b843e6437"></a>`tag_set_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [CollectionDescription](#s-95ff3511b7)
- [CollectionId](#s-99e4ae8314)

##### <a id="s-95ff3511b7"></a>definition `CollectionDescription`

- <a id="s-5ab20c2d8f"></a>`type`: `"string"`
- <a id="s-b0b2113572"></a>`maxLength`: `32768`
- <a id="s-694760481c"></a>`minLength`: `1`
- <a id="s-7ec846369f"></a>`x-riverhog-encoded-bytes-max`: `32768`
- <a id="s-90d6285466"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-catalog-description"}`
- <a id="s-7cf2b497ab"></a>`x-unicode-normalization`: `"NFC"`

##### <a id="s-99e4ae8314"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-131c8d4b9a"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-6bfca7987b"></a>2 | not=(const="0") |

## Governing policies

- <a id="pa-f1d1e08f7b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CatalogSyncUpsert`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0ca3a1b924bae0721d1afb243fc261ef8066a36c70aef8fb6fb10ebcfeb5f1d4 -->

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
    "signature": "\"(*, collection_id: CollectionId, archive_root_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], content_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], description: CollectionDescription | None, description_revision: Annotated[int, Strict(strict=True), Ge(ge=0), Le(le=9007199254740991)], description_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], tag_revision: Annotated[int, Strict(strict=True), Ge(ge=1), Le(le=9007199254740991)], tag_set_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], revision: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=19, pattern='^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18})$', ascii_only=None)], operation: Literal['upsert'] = 'upsert') -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CatalogSyncUpsert",
  "unit": "export"
}
```

</details>
