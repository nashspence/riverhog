# stove0_operator_contracts.AdmissionView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissionview:33b0586c5a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c22c4d3313"></a>
- <a id="s-584c6f5337"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-b38e807287"></a>`module`: `stove0_operator_contracts`
- <a id="s-498b858bfe"></a>`name`: `AdmissionView`
- <a id="s-94c1485c5c"></a>`unit`: `export`

### Declared structure

- <a id="s-d8475ceac3"></a>`kind`: `"class"`
- <a id="s-cb4c1bf6ee"></a>`signature`: `"\"(*, intent: stove0_operator_contracts.AdmissionIntent, state: Literal['intent', 'previewed', 'work_bound'], preview_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, work_id: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, attempt_count: Annotated[int, Ge(ge=0)], next_attempt_at: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=40)] = None, failure: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=1000)] = None, created_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=40)], updated_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=40)]) -> None\""`

#### Validated model schema

<a id="s-bebd933f68"></a>

- <a id="s-a19015f9c4"></a>`type`: `"object"`
- <a id="s-edede9e696"></a>`additionalProperties`: `false`
- <a id="s-a0b03e3514"></a>`required`: `["intent","state","attempt_count","created_at","updated_at"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-39f4aa867f"></a>`attempt_count` | yes | type="integer"; minimum=0 |  |
| <a id="s-ec2e41e1e4"></a>`created_at` | yes | type="string"; maxLength=40; minLength=1 |  |
| <a id="s-cc2e78584a"></a>`failure` | no | anyOf=(type="string"; maxLength=1000; minLength=1) \| (type="null"); default=null |  |
| <a id="s-24dc183dec"></a>`intent` | yes | [AdmissionIntent](#s-990313fa6e) |  |
| <a id="s-771f563f31"></a>`next_attempt_at` | no | anyOf=(type="string"; maxLength=40; minLength=1) \| (type="null"); default=null |  |
| <a id="s-6fdb4daaf7"></a>`preview_sha256` | no | anyOf=(type="string"; pattern="^[0-9a-f]{64}$") \| (type="null"); default=null |  |
| <a id="s-e916cc4872"></a>`state` | yes | type="string"; enum=["intent","previewed","work_bound"] |  |
| <a id="s-918983d3cd"></a>`updated_at` | yes | type="string"; maxLength=40; minLength=1 |  |
| <a id="s-280f1abd95"></a>`work_id` | no | anyOf=(type="string"; pattern="^[0-9a-f]{64}$") \| (type="null"); default=null |  |

##### Definitions

- [AdmissionIntent](#s-990313fa6e)
- [CatalogSyncDescriptor](#s-227a918078)
- [CollectionDescription](#s-c7eddb0819)
- [CollectionId](#s-8c5a60017f)
- [CollectionTag](#s-7119e58bd0)
- [JsonValue](#s-1179c21027)

##### <a id="s-990313fa6e"></a>definition `AdmissionIntent`

- <a id="s-bf5e8e4c58"></a>`type`: `"object"`
- <a id="s-a5fbe8cd1a"></a>`additionalProperties`: `false`
- <a id="s-ba834e2eab"></a>`required`: `["admission_id","policy_id","policy_revision","policy_sha256","required_tags","collection","recipe_id","recipe_revision","recipe_sha256","effective_intent"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b20df1840f"></a>`admission_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3d8673135f"></a>`collection` | yes | [CatalogSyncDescriptor](#s-227a918078) |  |
| <a id="s-1d0e47f6b1"></a>`effective_intent` | yes | type="object"; additionalProperties=([JsonValue](#s-1179c21027)) |  |
| <a id="s-6462558fe0"></a>`format` | no | type="string"; const="stove0-admission-intent/v1"; default="stove0-admission-intent/v1" |  |
| <a id="s-b8f0ac2ea2"></a>`policy_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-7e8bf505fe"></a>`policy_revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-0cdb0cd389"></a>`policy_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-55425f61f0"></a>`recipe_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-0c8c8de178"></a>`recipe_revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-fa9d66ea43"></a>`recipe_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-da89bdca80"></a>`required_tags` | yes | type="array"; items=([CollectionTag](#s-7119e58bd0)) |  |

##### <a id="s-227a918078"></a>definition `CatalogSyncDescriptor`

- <a id="s-9c54ec5858"></a>`type`: `"object"`
- <a id="s-1abbe16f99"></a>`additionalProperties`: `false`
- <a id="s-125323132e"></a>`required`: `["collection_id","archive_root_sha256","content_identity","description","description_revision","description_identity","tag_revision","tag_set_identity","revision"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bdeebe9cd5"></a>`archive_root_sha256` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-dbe44beb2b"></a>`collection_id` | yes | [CollectionId](#s-8c5a60017f) |  |
| <a id="s-eb6b0bdebb"></a>`content_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-989289d089"></a>`description` | yes | anyOf=([CollectionDescription](#s-c7eddb0819)) \| (type="null") |  |
| <a id="s-8e1c472a59"></a>`description_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-69796ee4eb"></a>`description_revision` | yes | type="integer"; minimum=0; maximum=9007199254740991 |  |
| <a id="s-c6593baf9d"></a>`revision` | yes | type="string"; maxLength=19; minLength=1; pattern="^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$" |  |
| <a id="s-2cc2df7ecd"></a>`tag_revision` | yes | type="integer"; minimum=1; maximum=9007199254740991 |  |
| <a id="s-767ba3ce65"></a>`tag_set_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-c7eddb0819"></a>definition `CollectionDescription`

- <a id="s-d9032ec900"></a>`type`: `"string"`
- <a id="s-50bd9b8ee1"></a>`maxLength`: `32768`
- <a id="s-1e8ed05a17"></a>`minLength`: `1`
- <a id="s-a03d48e048"></a>`x-riverhog-encoded-bytes-max`: `32768`
- <a id="s-e2bf8addb9"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-catalog-description"}`
- <a id="s-9112819a97"></a>`x-unicode-normalization`: `"NFC"`

##### <a id="s-8c5a60017f"></a>definition `CollectionId`

- <a id="s-b0450b2456"></a>`type`: `"integer"`
- <a id="s-f80305300c"></a>`minimum`: `1`

##### <a id="s-7119e58bd0"></a>definition `CollectionTag`

- <a id="s-f7fd35e3fb"></a>`type`: `"string"`
- <a id="s-05ef11ccb6"></a>`maxLength`: `65536`
- <a id="s-5f6fdbd8ee"></a>`minLength`: `1`
- <a id="s-6a427d87e1"></a>`x-riverhog-encoded-bytes-max`: `65536`
- <a id="s-91c16a8e3e"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-collection-tag"}`
- <a id="s-0a776e8fb8"></a>`x-unicode-normalization`: `"NFC"`

##### <a id="s-1179c21027"></a>definition `JsonValue`

- Accepts: any JSON value.

## Maintained corroboration

### Related interface records

- [exact_stage](stove0-operator-contracts-admissionview-exact-stage.md)

## Governing policies

- <a id="pa-1b2565cc2e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionView`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 237bed88eece97e9f6453ea273027629d7c677bbb45c8b367c76a4c26334f8fa -->

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
          "minimum": 1,
          "type": "integer"
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
        "attempt_count": {
          "minimum": 0,
          "type": "integer"
        },
        "created_at": {
          "maxLength": 40,
          "minLength": 1,
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
              "maxLength": 40,
              "minLength": 1,
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
          "maxLength": 40,
          "minLength": 1,
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
    "signature": "\"(*, intent: stove0_operator_contracts.AdmissionIntent, state: Literal['intent', 'previewed', 'work_bound'], preview_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, work_id: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, attempt_count: Annotated[int, Ge(ge=0)], next_attempt_at: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=40)] = None, failure: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=1000)] = None, created_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=40)], updated_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=40)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "AdmissionView",
  "unit": "export"
}
```

</details>
