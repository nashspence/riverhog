# stove0_operator_contracts.AdmissionPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissionpage:c08d98b36c -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-002aede269"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-952c714e0a"></a>`admissions` | yes | type="array"; items=(#/$defs/AdmissionView) |  |
| <a id="s-2948a285e9"></a>`filters` | yes | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-e3d29015b4"></a>`next_page_token` | yes | anyOf=#/$defs/BrowsePageToken \| type="null" |  |
| <a id="s-28c237f628"></a>`order` | yes | type="string"; enum=["asc","desc"] |  |
| <a id="s-b4ef672c5f"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| <a id="s-ac58678d09"></a>`policy_id` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-73752f95bc"></a>`sort` | yes | type="string"; enum=["created_at","updated_at","state","admission_id"] |  |
| <a id="s-02027fe057"></a>`state` | yes | anyOf=type="string"; enum=["intent","previewed","work_bound"] \| type="null" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-715abe15ea"></a>`AdmissionIntent` | type="object"; fields=`admission_id`, `collection`, `effective_intent`, `format`, `policy_id`, `policy_revision`, `policy_sha256`, `recipe_id`, `recipe_revision`, `recipe_sha256`, `required_tags`; additional keys=`additionalProperties`, `required` |
| <a id="s-fc72800bc8"></a>`AdmissionView` | type="object"; fields=`attempt_count`, `created_at`, `failure`, `intent`, `next_attempt_at`, `preview_sha256`, `state`, `updated_at`, `work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-dd3b10a08d"></a>`BrowsePageToken` | type="string"; minLength=1; maxLength=8192 |
| <a id="s-02586960a9"></a>`CatalogSyncDescriptor` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`, `description`, `description_identity`, `description_revision`, `revision`, `tag_revision`, `tag_set_identity`; additional keys=`additionalProperties`, `required` |
| <a id="s-fd5c292b93"></a>`CollectionDescription` | type="string"; minLength=1; maxLength=32768; additional keys=`x-riverhog-encoded-bytes-max`, `x-riverhog-extent`, `x-unicode-normalization` |
| <a id="s-a599deb515"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-660b7ac527"></a>`CollectionTag` | type="string"; minLength=1; maxLength=65536; additional keys=`x-riverhog-encoded-bytes-max`, `x-riverhog-extent`, `x-unicode-normalization` |
| <a id="s-d964e44e24"></a>`JsonValue` | empty object |

## Governing policies

- <a id="pa-3d604dd7a3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionPage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1518c0e1a71a8d05254ca76b446749dca5b7da6acb8e6fd3c18b8c4d85ea7915 -->

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
        "AdmissionView": {
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
