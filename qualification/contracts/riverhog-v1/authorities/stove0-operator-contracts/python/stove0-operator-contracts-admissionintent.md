# stove0_operator_contracts.AdmissionIntent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissionintent:dd55b3eb2a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7b4969ebdb"></a>
- <a id="s-b23fc88f3f"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-246851c59e"></a>`module`: `stove0_operator_contracts`
- <a id="s-f16cf6aca7"></a>`name`: `AdmissionIntent`
- <a id="s-800420742e"></a>`unit`: `export`

### Declared structure

- <a id="s-485b2e0a7c"></a>`kind`: `"class"`
- <a id="s-d5e1ed28d6"></a>`signature`: `"\"(*, format: Literal['stove0-admission-intent/v1'] = 'stove0-admission-intent/v1', admission_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], policy_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], policy_revision: Annotated[int, Ge(ge=1)], policy_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], required_tags: tuple[CollectionTag, ...], collection: riverhog_protocol.catalog_sync.CatalogSyncDescriptor, recipe_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], recipe_revision: Annotated[int, Ge(ge=1)], recipe_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], effective_intent: dict[str, JsonValue]) -> None\""`

#### Validated model schema

<a id="s-825ec0fc90"></a>

- <a id="s-41c6814b9d"></a>`type`: `"object"`
- <a id="s-fbb353d6c3"></a>`additionalProperties`: `false`
- <a id="s-2f1ab13f3e"></a>`required`: `["admission_id","policy_id","policy_revision","policy_sha256","required_tags","collection","recipe_id","recipe_revision","recipe_sha256","effective_intent"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9b47b9fd8d"></a>`admission_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ef4e52df48"></a>`collection` | yes | [CatalogSyncDescriptor](#s-becf941a62) |  |
| <a id="s-bd1b3c1f0d"></a>`effective_intent` | yes | type="object"; additionalProperties=([JsonValue](#s-87dae79148)) |  |
| <a id="s-d43901978a"></a>`format` | no | type="string"; const="stove0-admission-intent/v1"; default="stove0-admission-intent/v1" |  |
| <a id="s-89c9695f4f"></a>`policy_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-e6bd64bb79"></a>`policy_revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-b4296f7571"></a>`policy_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e2909a4b58"></a>`recipe_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-4d0773f3f1"></a>`recipe_revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-c1303526bb"></a>`recipe_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-10f74caced"></a>`required_tags` | yes | type="array"; items=([CollectionTag](#s-949d5d67c3)) |  |

##### Definitions

- [CatalogSyncDescriptor](#s-becf941a62)
- [CollectionDescription](#s-8da18b255b)
- [CollectionId](#s-581e1f826a)
- [CollectionTag](#s-949d5d67c3)
- [JsonValue](#s-87dae79148)

##### <a id="s-becf941a62"></a>definition `CatalogSyncDescriptor`

- <a id="s-1e6438568a"></a>`type`: `"object"`
- <a id="s-d8a4176dee"></a>`additionalProperties`: `false`
- <a id="s-8ed3924314"></a>`required`: `["collection_id","archive_root_sha256","content_identity","description","description_revision","description_identity","tag_revision","tag_set_identity","revision"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-12f3f18469"></a>`archive_root_sha256` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-839fe420cc"></a>`collection_id` | yes | [CollectionId](#s-581e1f826a) |  |
| <a id="s-a0b50003fb"></a>`content_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7458a633ef"></a>`description` | yes | anyOf=[([CollectionDescription](#s-8da18b255b)); (type="null")] |  |
| <a id="s-98f2eb76d8"></a>`description_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4ec2ec7d31"></a>`description_revision` | yes | type="integer"; minimum=0; maximum=9007199254740991 |  |
| <a id="s-0ab3f643c9"></a>`revision` | yes | type="string"; maxLength=19; minLength=1; pattern="^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$" |  |
| <a id="s-090ab4f91e"></a>`tag_revision` | yes | type="integer"; minimum=1; maximum=9007199254740991 |  |
| <a id="s-2b50a6b9d7"></a>`tag_set_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-8da18b255b"></a>definition `CollectionDescription`

- <a id="s-ca397ce749"></a>`type`: `"string"`
- <a id="s-3ee4db1c87"></a>`maxLength`: `32768`
- <a id="s-29636ea3b4"></a>`minLength`: `1`
- <a id="s-2a976cacd4"></a>`x-riverhog-encoded-bytes-max`: `32768`
- <a id="s-668f5c0de5"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-catalog-description"}`
- <a id="s-df9a378560"></a>`x-unicode-normalization`: `"NFC"`

##### <a id="s-581e1f826a"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-b46a160eb2"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-518ec420ea"></a>2 | not=(const="0") |

##### <a id="s-949d5d67c3"></a>definition `CollectionTag`

- <a id="s-118b8258dd"></a>`type`: `"string"`
- <a id="s-79d54a96c3"></a>`maxLength`: `65536`
- <a id="s-d0ee8f80b7"></a>`minLength`: `1`
- <a id="s-df04564ab1"></a>`x-riverhog-encoded-bytes-max`: `65536`
- <a id="s-545e19a67f"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-collection-tag"}`
- <a id="s-baeee27802"></a>`x-unicode-normalization`: `"NFC"`

##### <a id="s-87dae79148"></a>definition `JsonValue`

- Accepts: any JSON value.

## Maintained corroboration

### Related interface records

- [exact_identity](stove0-operator-contracts-admissionintent-exact-identity.md)
- [seal](stove0-operator-contracts-admissionintent-seal.md)

## Governing policies

- <a id="pa-66eae5b4ee"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionIntent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 40fd89aa27a487c7c3fe25a4df82e0fa92444f99aca7ea435d4d1eb2be86686d -->

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
        },
        "CollectionTag": {
          "maxLength": 65536,
          "minLength": 1,
          "type": "string",
          "x-riverhog-encoded-bytes-max": 65536,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-human-authored-collection-tag"
          },
          "x-unicode-normalization": "NFC"
        },
        "JsonValue": {}
      },
      "additionalProperties": false,
      "properties": {
        "admission_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "collection": {
          "$ref": "#/$defs/CatalogSyncDescriptor"
        },
        "effective_intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "type": "object"
        },
        "format": {
          "const": "stove0-admission-intent/v1",
          "default": "stove0-admission-intent/v1",
          "type": "string"
        },
        "policy_id": {
          "maxLength": 160,
          "minLength": 1,
          "type": "string"
        },
        "policy_revision": {
          "minimum": 1,
          "type": "integer"
        },
        "policy_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "recipe_id": {
          "maxLength": 160,
          "minLength": 1,
          "type": "string"
        },
        "recipe_revision": {
          "minimum": 1,
          "type": "integer"
        },
        "recipe_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "required_tags": {
          "items": {
            "$ref": "#/$defs/CollectionTag"
          },
          "type": "array"
        }
      },
      "required": [
        "admission_id",
        "policy_id",
        "policy_revision",
        "policy_sha256",
        "required_tags",
        "collection",
        "recipe_id",
        "recipe_revision",
        "recipe_sha256",
        "effective_intent"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-admission-intent/v1'] = 'stove0-admission-intent/v1', admission_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], policy_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], policy_revision: Annotated[int, Ge(ge=1)], policy_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], required_tags: tuple[CollectionTag, ...], collection: riverhog_protocol.catalog_sync.CatalogSyncDescriptor, recipe_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], recipe_revision: Annotated[int, Ge(ge=1)], recipe_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], effective_intent: dict[str, JsonValue]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "AdmissionIntent",
  "unit": "export"
}
```

</details>
