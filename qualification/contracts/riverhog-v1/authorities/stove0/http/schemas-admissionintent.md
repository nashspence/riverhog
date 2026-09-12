# schemas: AdmissionIntent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-admissionintent:ca5ce1169b -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 7 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/AdmissionIntent`

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

- [schemas: CatalogSyncDescriptor](schemas-catalogsyncdescriptor.md)
- [schemas: CollectionTag](schemas-collectiontag.md)
- [schemas: JsonValue](schemas-jsonvalue.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `contract_max` | maximum=160, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=160, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: AdmissionIntent
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `admission_id` | yes | string |  |
| `collection` | yes | #/components/schemas/CatalogSyncDescriptor |  |
| `effective_intent` | yes | object |  |
| `format` | no | string |  |
| `policy_id` | yes | string |  |
| `policy_revision` | yes | integer |  |
| `policy_sha256` | yes | string |  |
| `recipe_id` | yes | string |  |
| `recipe_revision` | yes | integer |  |
| `recipe_sha256` | yes | string |  |
| `required_tags` | yes | array |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0944c132ed593050435d503c92c7a9b57d07fea52ef8280ed7641f12f0c1f12f -->

```json
{
  "additionalProperties": false,
  "properties": {
    "admission_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Admission Id",
      "type": "string"
    },
    "collection": {
      "$ref": "#/components/schemas/CatalogSyncDescriptor"
    },
    "effective_intent": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
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
        "$ref": "#/components/schemas/CollectionTag"
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
}
```
