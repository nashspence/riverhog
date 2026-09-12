# schemas: AdmissionPolicy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-admissionpolicy:c8a66edb55 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/AdmissionPolicy`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CollectionTag](schemas-collectiontag.md)
- [schemas: JsonValue](schemas-jsonvalue.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `contract_max` | maximum=160, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `contract_max` | maximum=100, minimum=1, reason=bounded-exact-classification-admission-predicate |

## Contract summary

- `title`: AdmissionPolicy
- `description`: One bounded, exact all-of classification admission rule.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `automatic_preview` | no | string |  |
| `effective_intent` | no | object |  |
| `format` | no | string |  |
| `id` | yes | string |  |
| `recipe_id` | yes | string |  |
| `recipe_revision` | yes | integer |  |
| `recipe_sha256` | yes | string |  |
| `required_tags` | yes | array |  |
| `revision` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b922dd931b907481cef3ecf609f8bc3f55ee33cb8bf1736c53e552822b5a43b5 -->

```json
{
  "additionalProperties": false,
  "description": "One bounded, exact all-of classification admission rule.",
  "properties": {
    "automatic_preview": {
      "const": "accept-ready",
      "default": "accept-ready",
      "title": "Automatic Preview",
      "type": "string"
    },
    "effective_intent": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Effective Intent",
      "type": "object"
    },
    "format": {
      "const": "stove0-admission-policy/v1",
      "default": "stove0-admission-policy/v1",
      "title": "Format",
      "type": "string"
    },
    "id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$",
      "title": "Id",
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
        "$ref": "#/components/schemas/CollectionTag"
      },
      "maxItems": 100,
      "minItems": 1,
      "title": "Required Tags",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "contract_max",
        "reason": "bounded-exact-classification-admission-predicate"
      }
    },
    "revision": {
      "minimum": 1,
      "title": "Revision",
      "type": "integer"
    }
  },
  "required": [
    "id",
    "revision",
    "required_tags",
    "recipe_id",
    "recipe_revision",
    "recipe_sha256"
  ],
  "title": "AdmissionPolicy",
  "type": "object"
}
```
