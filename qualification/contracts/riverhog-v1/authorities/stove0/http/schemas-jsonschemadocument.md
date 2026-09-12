# schemas: JsonSchemaDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-jsonschemadocument:51d2ea1b40 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/JsonSchemaDocument`

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

- [schemas: JsonValue](schemas-jsonvalue.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: JsonSchemaDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `dialect` | no | string |  |
| `format_policy` | no | string |  |
| `id` | yes | string |  |
| `schema` | yes | object |  |
| `sha256` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c25c5c6212e632c14997394e8acd4f3c9429f84a386cd88fb9f7fcbc78128bd5 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "dialect": {
      "const": "https://json-schema.org/draft/2020-12/schema",
      "default": "https://json-schema.org/draft/2020-12/schema",
      "title": "Dialect",
      "type": "string"
    },
    "format_policy": {
      "const": "annotation-only",
      "default": "annotation-only",
      "title": "Format Policy",
      "type": "string"
    },
    "id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Id",
      "type": "string"
    },
    "schema": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Schema",
      "type": "object"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    }
  },
  "required": [
    "id",
    "sha256",
    "schema"
  ],
  "title": "JsonSchemaDocument",
  "type": "object"
}
```
