# schemas: OutcomeSetDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-outcomesetdocument:6be204ed92 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/OutcomeSetDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ExactSetAuthorityDocument](schemas-exactsetauthoritydocument.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `contract_max` | maximum=1000, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: OutcomeSetDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `authority` | no | object (1 fields) |  |
| `count` | yes | integer |  |
| `failure` | no | object (2 fields) |  |
| `state` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8576ac8304a0301e16f6f81bd65ae1e818011299bdb236ea4db30959d6365dc5 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "authority": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ExactSetAuthorityDocument"
        },
        {
          "type": "null"
        }
      ]
    },
    "count": {
      "minimum": 0,
      "title": "Count",
      "type": "integer"
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
      "title": "Failure"
    },
    "state": {
      "enum": [
        "receiving",
        "sealing",
        "sealed",
        "failed"
      ],
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "state",
    "count"
  ],
  "title": "OutcomeSetDocument",
  "type": "object"
}
```
