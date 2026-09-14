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
- <a id="s-4ac3392872"></a>`title`: AdmissionView
- <a id="s-a19015f9c4"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-39f4aa867f"></a>`attempt_count` | yes | type="integer"; minimum=0 |  |
| <a id="s-ec2e41e1e4"></a>`created_at` | yes | type="string"; minLength=1; maxLength=40 |  |
| <a id="s-cc2e78584a"></a>`failure` | no | anyOf=type="string"; minLength=1; maxLength=1000 \| type="null" |  |
| <a id="s-24dc183dec"></a>`intent` | yes | #/$defs/AdmissionIntent |  |
| <a id="s-771f563f31"></a>`next_attempt_at` | no | anyOf=type="string"; minLength=1; maxLength=40 \| type="null" |  |
| <a id="s-6fdb4daaf7"></a>`preview_sha256` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-e916cc4872"></a>`state` | yes | type="string"; enum=["intent","previewed","work_bound"] |  |
| <a id="s-918983d3cd"></a>`updated_at` | yes | type="string"; minLength=1; maxLength=40 |  |
| <a id="s-280f1abd95"></a>`work_id` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-990313fa6e"></a>`AdmissionIntent` | type="object"; fields=`admission_id`, `collection`, `effective_intent`, `format`, `policy_id`, `policy_revision`, `policy_sha256`, `recipe_id`, `recipe_revision`, `recipe_sha256`, `required_tags`; additional keys=`additionalProperties`, `required` |
| <a id="s-227a918078"></a>`CatalogSyncDescriptor` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`, `description`, `description_identity`, `description_revision`, `revision`, `tag_revision`, `tag_set_identity`; additional keys=`additionalProperties`, `required` |
| <a id="s-c7eddb0819"></a>`CollectionDescription` | type="string"; minLength=1; maxLength=32768; additional keys=`x-riverhog-encoded-bytes-max`, `x-riverhog-extent`, `x-unicode-normalization` |
| <a id="s-8c5a60017f"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-7119e58bd0"></a>`CollectionTag` | type="string"; minLength=1; maxLength=65536; additional keys=`x-riverhog-encoded-bytes-max`, `x-riverhog-extent`, `x-unicode-normalization` |
| <a id="s-1179c21027"></a>`JsonValue` | empty object |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.AdmissionView.exact_stage](stove0-operator-contracts-admissionview-exact-stage.md)

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

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2273407f3b19d3f2d39fe0a0678944a11ede7c1b283201229911fcb0b7bfd5e6 -->

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
              "title": "Admission Id",
              "type": "string"
            },
            "collection": {
              "$ref": "#/$defs/CatalogSyncDescriptor"
            },
            "effective_intent": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "title": "Effective Intent",
              "type": "object"
            },
            "format": {
              "const": "stove0-admission-intent/v1",
              "default": "stove0-admission-intent/v1",
              "title": "Format",
              "type": "string"
            },
            "policy_id": {
              "maxLength": 160,
              "minLength": 1,
              "title": "Policy Id",
              "type": "string"
            },
            "policy_revision": {
              "minimum": 1,
              "title": "Policy Revision",
              "type": "integer"
            },
            "policy_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Policy Sha256",
              "type": "string"
            },
            "recipe_id": {
              "maxLength": 160,
              "minLength": 1,
              "title": "Recipe Id",
              "type": "string"
            },
            "recipe_revision": {
              "minimum": 1,
              "title": "Recipe Revision",
              "type": "integer"
            },
            "recipe_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Recipe Sha256",
              "type": "string"
            },
            "required_tags": {
              "items": {
                "$ref": "#/$defs/CollectionTag"
              },
              "title": "Required Tags",
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
          "title": "AdmissionIntent",
          "type": "object"
        },
        "CatalogSyncDescriptor": {
          "additionalProperties": false,
          "properties": {
            "archive_root_sha256": {
              "maxLength": 64,
              "minLength": 64,
              "pattern": "^[0-9a-f]{64}$",
              "title": "Archive Root Sha256",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "content_identity": {
              "maxLength": 64,
              "minLength": 64,
              "pattern": "^[0-9a-f]{64}$",
              "title": "Content Identity",
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
              "title": "Description Identity",
              "type": "string"
            },
            "description_revision": {
              "maximum": 9007199254740991,
              "minimum": 0,
              "title": "Description Revision",
              "type": "integer"
            },
            "revision": {
              "maxLength": 19,
              "minLength": 1,
              "pattern": "^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18})$",
              "title": "Revision",
              "type": "string"
            },
            "tag_revision": {
              "maximum": 9007199254740991,
              "minimum": 1,
              "title": "Tag Revision",
              "type": "integer"
            },
            "tag_set_identity": {
              "maxLength": 64,
              "minLength": 64,
              "pattern": "^[0-9a-f]{64}$",
              "title": "Tag Set Identity",
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
          "title": "CatalogSyncDescriptor",
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
          "title": "Attempt Count",
          "type": "integer"
        },
        "created_at": {
          "maxLength": 40,
          "minLength": 1,
          "title": "Created At",
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
          "default": null,
          "title": "Failure"
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
          "default": null,
          "title": "Next Attempt At"
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
          "default": null,
          "title": "Preview Sha256"
        },
        "state": {
          "enum": [
            "intent",
            "previewed",
            "work_bound"
          ],
          "title": "State",
          "type": "string"
        },
        "updated_at": {
          "maxLength": 40,
          "minLength": 1,
          "title": "Updated At",
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
          "default": null,
          "title": "Work Id"
        }
      },
      "required": [
        "intent",
        "state",
        "attempt_count",
        "created_at",
        "updated_at"
      ],
      "title": "AdmissionView",
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
