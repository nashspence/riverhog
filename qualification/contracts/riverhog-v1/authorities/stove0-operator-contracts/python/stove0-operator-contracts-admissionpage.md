# stove0_operator_contracts.AdmissionPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissionpage:c08d98b36c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-996bf7deab"></a>
- <a id="s-c04bead7de"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-861ee0bcef"></a>`module`: `stove0_operator_contracts`
- <a id="s-7cb454134d"></a>`name`: `AdmissionPage`
- <a id="s-561a6b0083"></a>`unit`: `export`

### Declared structure

- <a id="s-c278f8c2a0"></a>`kind`: `"class"`
- <a id="s-9a9a9e6761"></a>`signature`: `"\"(*, page_size: Annotated[int, Ge(ge=1), Le(le=100)], next_page_token: BrowsePageToken \| None, sort: Literal['created_at', 'updated_at', 'state', 'admission_id'], order: Literal['asc', 'desc'], filters: dict[str, JsonValue], policy_id: str \| None, state: Optional[Literal['intent', 'previewed', 'work_bound']], admissions: tuple[stove0_operator_contracts.AdmissionView, ...]) -> None\""`

#### Validated model schema

<a id="s-e19189015e"></a>

- <a id="s-002aede269"></a>`type`: `"object"`
- <a id="s-24b837ebd6"></a>`additionalProperties`: `false`
- <a id="s-000ec98b15"></a>`required`: `["page_size","next_page_token","sort","order","filters","policy_id","state","admissions"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-952c714e0a"></a>`admissions` | yes | type="array"; items=([AdmissionView](#s-fc72800bc8)) |  |
| <a id="s-2948a285e9"></a>`filters` | yes | type="object"; additionalProperties=([JsonValue](#s-d964e44e24)) |  |
| <a id="s-e3d29015b4"></a>`next_page_token` | yes | anyOf=[([BrowsePageToken](#s-dd3b10a08d)); (type="null")] |  |
| <a id="s-28c237f628"></a>`order` | yes | type="string"; enum=["asc","desc"] |  |
| <a id="s-b4ef672c5f"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| <a id="s-ac58678d09"></a>`policy_id` | yes | anyOf=[(type="string"); (type="null")] |  |
| <a id="s-73752f95bc"></a>`sort` | yes | type="string"; enum=["created_at","updated_at","state","admission_id"] |  |
| <a id="s-02027fe057"></a>`state` | yes | anyOf=[(type="string"; enum=["intent","previewed","work_bound"]); (type="null")] |  |

##### Definitions

- [AdmissionIntent](#s-715abe15ea)
- [AdmissionView](#s-fc72800bc8)
- [AllVisibleAdmissionSelector](#s-84d56c2dac)
- [BrowsePageToken](#s-dd3b10a08d)
- [CatalogSyncDescriptor](#s-02586960a9)
- [CollectionDescription](#s-fd5c292b93)
- [CollectionId](#s-a599deb515)
- [CollectionTag](#s-660b7ac527)
- [JsonValue](#s-d964e44e24)
- [NonnegativeDecimal](#s-1041b08a94)
- [TaggedAdmissionSelector](#s-c994665bfd)

##### <a id="s-715abe15ea"></a>definition `AdmissionIntent`

- <a id="s-06f182dd0d"></a>`type`: `"object"`
- <a id="s-855ebda303"></a>`additionalProperties`: `false`
- <a id="s-b4b62093c4"></a>`required`: `["admission_id","policy_id","policy_revision","policy_sha256","selector","collection","recipe_id","recipe_revision","recipe_sha256","effective_intent"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8387295f86"></a>`admission_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3d0c8ea53f"></a>`collection` | yes | [CatalogSyncDescriptor](#s-02586960a9) |  |
| <a id="s-cd29887a20"></a>`effective_intent` | yes | type="object"; additionalProperties=([JsonValue](#s-d964e44e24)) |  |
| <a id="s-24d3946761"></a>`format` | no | type="string"; const="stove0-admission-intent/v1"; default="stove0-admission-intent/v1" |  |
| <a id="s-db3b3bf52e"></a>`policy_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-5c376a0409"></a>`policy_revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-e0d76b9774"></a>`policy_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9f7e272724"></a>`recipe_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-c7d1409453"></a>`recipe_revision` | yes | [NonnegativeDecimal](#s-1041b08a94); ge=1 |  |
| <a id="s-5516b1d011"></a>`recipe_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-efa4ff87ff"></a>`selector` | yes | discriminator={"mapping":{"all":"#/$defs/AllVisibleAdmissionSelector","tags":"#/$defs/TaggedAdmissionSelector"},"propertyName":"kind"}; oneOf=[([AllVisibleAdmissionSelector](#s-84d56c2dac)); ([TaggedAdmissionSelector](#s-c994665bfd))] |  |

##### <a id="s-fc72800bc8"></a>definition `AdmissionView`

- <a id="s-92c080f8a2"></a>`type`: `"object"`
- <a id="s-958d60a293"></a>`additionalProperties`: `false`
- <a id="s-d7009abe8c"></a>`required`: `["intent","state","attempt_count","created_at","updated_at"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6eebed3c01"></a>`attempt_count` | yes | type="integer"; minimum=0 |  |
| <a id="s-6bf0141e7c"></a>`created_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-93fba00a1c"></a>`failure` | no | anyOf=[(type="string"; maxLength=1000; minLength=1); (type="null")]; default=null |  |
| <a id="s-5600181fb9"></a>`intent` | yes | [AdmissionIntent](#s-715abe15ea) |  |
| <a id="s-5d21396028"></a>`next_attempt_at` | no | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; default=null |  |
| <a id="s-12e9d8c692"></a>`preview_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-966df76680"></a>`state` | yes | type="string"; enum=["intent","previewed","work_bound"] |  |
| <a id="s-0ff1be1354"></a>`updated_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-fe6b190aa1"></a>`work_id` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |

##### <a id="s-84d56c2dac"></a>definition `AllVisibleAdmissionSelector`

- <a id="s-74e20a33de"></a>`type`: `"object"`
- <a id="s-ce28c71f6f"></a>`additionalProperties`: `false`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a6f8ce4fd3"></a>`kind` | no | type="string"; const="all"; default="all" |  |

##### <a id="s-dd3b10a08d"></a>definition `BrowsePageToken`

- <a id="s-51a8deb2dd"></a>`type`: `"string"`
- <a id="s-4b39ec4e42"></a>`maxLength`: `8192`
- <a id="s-2430022bca"></a>`minLength`: `1`

##### <a id="s-02586960a9"></a>definition `CatalogSyncDescriptor`

- <a id="s-baeadbb9e4"></a>`type`: `"object"`
- <a id="s-cf0b0869f6"></a>`additionalProperties`: `false`
- <a id="s-76af7b3bcb"></a>`required`: `["collection_id","archive_root_sha256","content_identity","description","description_revision","description_identity","tag_revision","tag_set_identity","revision"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-090771ae58"></a>`archive_root_sha256` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c1bcd8f367"></a>`collection_id` | yes | [CollectionId](#s-a599deb515) |  |
| <a id="s-f8fb8db6c8"></a>`content_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9cea4d339d"></a>`description` | yes | anyOf=[([CollectionDescription](#s-fd5c292b93)); (type="null")] |  |
| <a id="s-711b538401"></a>`description_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1dc677d28b"></a>`description_revision` | yes | type="integer"; minimum=0; maximum=9007199254740991 |  |
| <a id="s-984c3d6b23"></a>`revision` | yes | type="string"; maxLength=19; minLength=1; pattern="^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$" |  |
| <a id="s-9310aee3c6"></a>`tag_revision` | yes | type="integer"; minimum=1; maximum=9007199254740991 |  |
| <a id="s-2e52a281bb"></a>`tag_set_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-fd5c292b93"></a>definition `CollectionDescription`

- <a id="s-279b204084"></a>`type`: `"string"`
- <a id="s-d471011064"></a>`maxLength`: `32768`
- <a id="s-6fb59e8277"></a>`minLength`: `1`
- <a id="s-3ee3e5f6c0"></a>`x-riverhog-encoded-bytes-max`: `32768`
- <a id="s-5edd76b2d2"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-catalog-description"}`
- <a id="s-753b7ee677"></a>`x-unicode-normalization`: `"NFC"`

##### <a id="s-a599deb515"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-4ab80df975"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-ed47544554"></a>2 | not=(const="0") |

##### <a id="s-660b7ac527"></a>definition `CollectionTag`

- <a id="s-d4df5cdfea"></a>`type`: `"string"`
- <a id="s-c252c52333"></a>`maxLength`: `65536`
- <a id="s-d834065e61"></a>`minLength`: `1`
- <a id="s-3c5497d725"></a>`x-riverhog-encoded-bytes-max`: `65536`
- <a id="s-13f93483ca"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-collection-tag"}`
- <a id="s-c127b87629"></a>`x-unicode-normalization`: `"NFC"`

##### <a id="s-d964e44e24"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-1041b08a94"></a>definition `NonnegativeDecimal`

- <a id="s-67be15ad79"></a>`type`: `"string"`
- <a id="s-5138e8577a"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

##### <a id="s-c994665bfd"></a>definition `TaggedAdmissionSelector`

- <a id="s-84132d1c05"></a>`type`: `"object"`
- <a id="s-e0138193f3"></a>`additionalProperties`: `false`
- <a id="s-d410c57a84"></a>`required`: `["required"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-22a37e7de0"></a>`kind` | no | type="string"; const="tags"; default="tags" |  |
| <a id="s-d2b2718a5b"></a>`required` | yes | type="array"; items=([CollectionTag](#s-660b7ac527)); maxItems=100; minItems=1; x-riverhog-extent={"policy":"contract_max","reason":"bounded-exact-classification-admission-predicate"} |  |

## Governing policies

- <a id="pa-3d604dd7a3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionPage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 26f1b099119fbf782f59c22a49af0ebbdd6ede4d6f9412aa9143648be0256197 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "AdmissionIntent": {
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
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 1
            },
            "recipe_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "selector": {
              "discriminator": {
                "mapping": {
                  "all": "#/$defs/AllVisibleAdmissionSelector",
                  "tags": "#/$defs/TaggedAdmissionSelector"
                },
                "propertyName": "kind"
              },
              "oneOf": [
                {
                  "$ref": "#/$defs/AllVisibleAdmissionSelector"
                },
                {
                  "$ref": "#/$defs/TaggedAdmissionSelector"
                }
              ]
            }
          },
          "required": [
            "admission_id",
            "policy_id",
            "policy_revision",
            "policy_sha256",
            "selector",
            "collection",
            "recipe_id",
            "recipe_revision",
            "recipe_sha256",
            "effective_intent"
          ],
          "type": "object"
        },
        "AdmissionView": {
          "additionalProperties": false,
          "properties": {
            "attempt_count": {
              "minimum": 0,
              "type": "integer"
            },
            "created_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "failure": {
              "anyOf": [
                {
                  "maxLength": 1000,
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "intent": {
              "$ref": "#/$defs/AdmissionIntent"
            },
            "next_attempt_at": {
              "anyOf": [
                {
                  "maxLength": 30,
                  "minLength": 30,
                  "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "preview_sha256": {
              "anyOf": [
                {
                  "pattern": "^[0-9a-f]{64}$",
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "state": {
              "enum": [
                "intent",
                "previewed",
                "work_bound"
              ],
              "type": "string"
            },
            "updated_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "work_id": {
              "anyOf": [
                {
                  "pattern": "^[0-9a-f]{64}$",
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            }
          },
          "required": [
            "intent",
            "state",
            "attempt_count",
            "created_at",
            "updated_at"
          ],
          "type": "object"
        },
        "AllVisibleAdmissionSelector": {
          "additionalProperties": false,
          "properties": {
            "kind": {
              "const": "all",
              "default": "all",
              "type": "string"
            }
          },
          "type": "object"
        },
        "BrowsePageToken": {
          "maxLength": 8192,
          "minLength": 1,
          "type": "string"
        },
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
        "JsonValue": {},
        "NonnegativeDecimal": {
          "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
          "type": "string"
        },
        "TaggedAdmissionSelector": {
          "additionalProperties": false,
          "properties": {
            "kind": {
              "const": "tags",
              "default": "tags",
              "type": "string"
            },
            "required": {
              "items": {
                "$ref": "#/$defs/CollectionTag"
              },
              "maxItems": 100,
              "minItems": 1,
              "type": "array",
              "x-riverhog-extent": {
                "policy": "contract_max",
                "reason": "bounded-exact-classification-admission-predicate"
              }
            }
          },
          "required": [
            "required"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "admissions": {
          "items": {
            "$ref": "#/$defs/AdmissionView"
          },
          "type": "array"
        },
        "filters": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "type": "object"
        },
        "next_page_token": {
          "anyOf": [
            {
              "$ref": "#/$defs/BrowsePageToken"
            },
            {
              "type": "null"
            }
          ]
        },
        "order": {
          "enum": [
            "asc",
            "desc"
          ],
          "type": "string"
        },
        "page_size": {
          "maximum": 100,
          "minimum": 1,
          "type": "integer"
        },
        "policy_id": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ]
        },
        "sort": {
          "enum": [
            "created_at",
            "updated_at",
            "state",
            "admission_id"
          ],
          "type": "string"
        },
        "state": {
          "anyOf": [
            {
              "enum": [
                "intent",
                "previewed",
                "work_bound"
              ],
              "type": "string"
            },
            {
              "type": "null"
            }
          ]
        }
      },
      "required": [
        "page_size",
        "next_page_token",
        "sort",
        "order",
        "filters",
        "policy_id",
        "state",
        "admissions"
      ],
      "type": "object"
    },
    "signature": "\"(*, page_size: Annotated[int, Ge(ge=1), Le(le=100)], next_page_token: BrowsePageToken | None, sort: Literal['created_at', 'updated_at', 'state', 'admission_id'], order: Literal['asc', 'desc'], filters: dict[str, JsonValue], policy_id: str | None, state: Optional[Literal['intent', 'previewed', 'work_bound']], admissions: tuple[stove0_operator_contracts.AdmissionView, ...]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "AdmissionPage",
  "unit": "export"
}
```

</details>
