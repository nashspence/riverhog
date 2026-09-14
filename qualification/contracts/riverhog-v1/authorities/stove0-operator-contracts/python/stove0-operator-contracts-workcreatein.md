# stove0_operator_contracts.WorkCreateIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workcreatein:346ce08e5c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-19eca1b23f"></a>
- <a id="s-d020902aca"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-8b38ce800c"></a>`module`: `stove0_operator_contracts`
- <a id="s-2ac58b77aa"></a>`name`: `WorkCreateIn`
- <a id="s-7e7064a029"></a>`unit`: `export`

### Declared structure

- <a id="s-0b1da7d09b"></a>`kind`: `"class"`
- <a id="s-c5b1033def"></a>`signature`: `"\"(*, recipe_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], recipe_revision: Annotated[int \| None, Ge(ge=1)] = None, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootRef, ...], MinLen(min_length=1)], effective_intent: dict[str, JsonValue] = <factory>, preview_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-b63bbb84b7"></a>
- <a id="s-f9e7faea31"></a>`title`: WorkCreateIn
- <a id="s-6195678c95"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dd2307aa0d"></a>`effective_intent` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-4c95719f45"></a>`inputs` | yes | type="array"; minItems=1; items=(#/$defs/CollectionRootRef) |  |
| <a id="s-eaf26ba496"></a>`preview_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-268a9b87b2"></a>`recipe_id` | yes | type="string"; minLength=1; maxLength=160 |  |
| <a id="s-5e47a00c0f"></a>`recipe_revision` | no | anyOf=type="integer"; minimum=1 \| type="null" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-f6865d067f"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-efa4ba92d2"></a>`CollectionRootRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`; additional keys=`additionalProperties`, `required` |
| <a id="s-ed02433ff9"></a>`JsonValue` | empty object |

## Governing policies

- <a id="pa-566c0baa8d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkCreateIn`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cea36d6fe509d94fd41ad2115bf502bc873065c1a81a1a33352d9e858a4ec3f7 -->

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
        "CollectionRootRef": {
          "additionalProperties": false,
          "properties": {
            "archive_root_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Archive Root Sha256",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "content_identity": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Content Identity",
              "type": "string"
            }
          },
          "required": [
            "collection_id",
            "archive_root_sha256",
            "content_identity"
          ],
          "title": "CollectionRootRef",
          "type": "object"
        },
        "JsonValue": {}
      },
      "additionalProperties": false,
      "properties": {
        "effective_intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Effective Intent",
          "type": "object"
        },
        "inputs": {
          "items": {
            "$ref": "#/$defs/CollectionRootRef"
          },
          "minItems": 1,
          "title": "Inputs",
          "type": "array"
        },
        "preview_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Preview Sha256",
          "type": "string"
        },
        "recipe_id": {
          "maxLength": 160,
          "minLength": 1,
          "title": "Recipe Id",
          "type": "string"
        },
        "recipe_revision": {
          "anyOf": [
            {
              "minimum": 1,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Recipe Revision"
        }
      },
      "required": [
        "recipe_id",
        "inputs",
        "preview_sha256"
      ],
      "title": "WorkCreateIn",
      "type": "object"
    },
    "signature": "\"(*, recipe_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], recipe_revision: Annotated[int | None, Ge(ge=1)] = None, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootRef, ...], MinLen(min_length=1)], effective_intent: dict[str, JsonValue] = <factory>, preview_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "WorkCreateIn",
  "unit": "export"
}
```
