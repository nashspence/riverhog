# stove0_target_protocol.DepartureEffectIntentPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-departureeffectintentpayload:46abfc1cbd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5d706acfbe"></a>
- <a id="s-9efa888b69"></a>`distribution`: `stove0-target-protocol`
- <a id="s-3ae27470f9"></a>`module`: `stove0_target_protocol`
- <a id="s-d7131da80c"></a>`name`: `DepartureEffectIntentPayload`
- <a id="s-e9a86d3b6a"></a>`unit`: `export`

### Declared structure

- <a id="s-2543cdbe2b"></a>`kind`: `"class"`
- <a id="s-4734c5a4b2"></a>`signature`: `"\"(*, format: Literal['stove0-departure-effect-intent/v1'] = 'stove0-departure-effect-intent/v1', policy_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$')], policy_revision: Annotated[int, Ge(ge=1)], policy_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], target_registration_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], target_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], source_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], authorization_view_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], last_collection: riverhog_protocol.catalog_sync.CatalogSyncDescriptor, departure_cause: Literal['collection_deleted', 'visibility_lost'], departure_revision: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=19, pattern='^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-c6e00c2ca2"></a>

- <a id="s-785238318a"></a>`type`: `"object"`
- <a id="s-9fca35a0d2"></a>`additionalProperties`: `false`
- <a id="s-cc06917bee"></a>`required`: `["policy_id","policy_revision","policy_sha256","target_registration_id","target_identity","source_identity","authorization_view_identity","last_collection","departure_cause","departure_revision"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9d7913f7fb"></a>`authorization_view_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-be175e7297"></a>`departure_cause` | yes | type="string"; enum=["collection_deleted","visibility_lost"] |  |
| <a id="s-ad7d393bf4"></a>`departure_revision` | yes | type="string"; maxLength=19; minLength=1; pattern="^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$" |  |
| <a id="s-6e2b4e5f39"></a>`format` | no | type="string"; const="stove0-departure-effect-intent/v1"; default="stove0-departure-effect-intent/v1" |  |
| <a id="s-30a3a48293"></a>`last_collection` | yes | [CatalogSyncDescriptor](#s-5402955f75) |  |
| <a id="s-637039535f"></a>`policy_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d4eb3ab0de"></a>`policy_revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-f6e8cbf79e"></a>`policy_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9590e394ff"></a>`source_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b57f6f54b2"></a>`target_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5e1cd0b52f"></a>`target_registration_id` | yes | type="string"; maxLength=160; minLength=1 |  |

##### Definitions

- [CatalogSyncDescriptor](#s-5402955f75)
- [CollectionDescription](#s-5453c4e684)
- [CollectionId](#s-8af81cd1e6)

##### <a id="s-5402955f75"></a>definition `CatalogSyncDescriptor`

- <a id="s-92e89d662c"></a>`type`: `"object"`
- <a id="s-85103b3bb7"></a>`additionalProperties`: `false`
- <a id="s-7f6cd8de12"></a>`required`: `["collection_id","archive_root_sha256","content_identity","description","description_revision","description_identity","tag_revision","tag_set_identity","revision"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c64370eaf7"></a>`archive_root_sha256` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7683daba14"></a>`collection_id` | yes | [CollectionId](#s-8af81cd1e6) |  |
| <a id="s-df1cbb7b03"></a>`content_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-75732e7d7e"></a>`description` | yes | anyOf=[([CollectionDescription](#s-5453c4e684)); (type="null")] |  |
| <a id="s-06cda016d6"></a>`description_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c2b79ea99a"></a>`description_revision` | yes | type="integer"; minimum=0; maximum=9007199254740991 |  |
| <a id="s-4d5373c214"></a>`revision` | yes | type="string"; maxLength=19; minLength=1; pattern="^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$" |  |
| <a id="s-130f0e6441"></a>`tag_revision` | yes | type="integer"; minimum=1; maximum=9007199254740991 |  |
| <a id="s-46979cd849"></a>`tag_set_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-5453c4e684"></a>definition `CollectionDescription`

- <a id="s-1a80e5223c"></a>`type`: `"string"`
- <a id="s-0b91138c5c"></a>`maxLength`: `32768`
- <a id="s-52b1ab490c"></a>`minLength`: `1`
- <a id="s-0030c14359"></a>`x-riverhog-encoded-bytes-max`: `32768`
- <a id="s-5662098151"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-catalog-description"}`
- <a id="s-d4fbe7bff7"></a>`x-unicode-normalization`: `"NFC"`

##### <a id="s-8af81cd1e6"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-cd8be865f1"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-7521d19b3e"></a>2 | not=(const="0") |

## Maintained corroboration

### Related interface records

- [later_than_last_seen](stove0-target-protocol-departureeffectintentpayload-later-than-last-seen.md)

## Governing policies

- <a id="pa-2f6b7d40b6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.DepartureEffectIntentPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e4b02c1e876ada023a68c4c649b5c4c56962de414d716a22660fc2883f7fbdab -->

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
        "departure_cause": {
          "enum": [
            "collection_deleted",
            "visibility_lost"
          ],
          "type": "string"
        },
        "departure_revision": {
          "maxLength": 19,
          "minLength": 1,
          "pattern": "^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18})$",
          "type": "string"
        },
        "format": {
          "const": "stove0-departure-effect-intent/v1",
          "default": "stove0-departure-effect-intent/v1",
          "type": "string"
        },
        "last_collection": {
          "$ref": "#/$defs/CatalogSyncDescriptor"
        },
        "policy_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$",
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
        "source_identity": {
          "maxLength": 64,
          "minLength": 64,
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "target_identity": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "target_registration_id": {
          "maxLength": 160,
          "minLength": 1,
          "type": "string"
        }
      },
      "required": [
        "policy_id",
        "policy_revision",
        "policy_sha256",
        "target_registration_id",
        "target_identity",
        "source_identity",
        "authorization_view_identity",
        "last_collection",
        "departure_cause",
        "departure_revision"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-departure-effect-intent/v1'] = 'stove0-departure-effect-intent/v1', policy_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$')], policy_revision: Annotated[int, Ge(ge=1)], policy_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], target_registration_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], target_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], source_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], authorization_view_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], last_collection: riverhog_protocol.catalog_sync.CatalogSyncDescriptor, departure_cause: Literal['collection_deleted', 'visibility_lost'], departure_revision: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=19, pattern='^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18})$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "DepartureEffectIntentPayload",
  "unit": "export"
}
```

</details>
