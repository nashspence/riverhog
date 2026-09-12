# schemas: ApplicationAccessGrantSet

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-applicationaccessgrantset:1bd6dc5665 -->

A nonempty, duplicate-free public grant set with canonical wildcard use.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

- `title`: ApplicationAccessGrantSet
- `description`: A nonempty, duplicate-free public grant set with canonical wildcard use.
- `type`: array

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | items | `contract_max` | maximum=1, reason=wildcard-access-grant-is-exclusive |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ApplicationAccessGrant](schemas-applicationaccessgrant.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ApplicationAccessGrantSet`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5133bc9028edbe95e0ad3ec9b476cda4bdb9c92a1c4942973660db9d1df5bd06 -->

```json
{
  "allOf": [
    {
      "if": {
        "contains": {
          "properties": {
            "permission": {
              "const": "*"
            }
          },
          "required": [
            "permission"
          ],
          "type": "object"
        }
      },
      "then": {
        "maxItems": 1,
        "x-riverhog-extent": {
          "policy": "contract_max",
          "reason": "wildcard-access-grant-is-exclusive"
        }
      }
    }
  ],
  "description": "A nonempty, duplicate-free public grant set with canonical wildcard use.",
  "items": {
    "$ref": "#/components/schemas/ApplicationAccessGrant"
  },
  "minItems": 1,
  "title": "ApplicationAccessGrantSet",
  "type": "array",
  "uniqueItems": true
}
```
