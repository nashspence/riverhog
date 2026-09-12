# schemas: ReceivingSetDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-receivingsetdocument:d32cb4d9f3 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ReceivingSetDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`

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

## Contract summary

- `title`: ReceivingSetDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `authority` | no | object (1 fields) |  |
| `count` | yes | integer |  |
| `state` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bdae856bb3b833bd968c3093868c21529535df0ab22642823b261aeb4d63f282 -->

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
    "state": {
      "enum": [
        "receiving",
        "sealed"
      ],
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "state",
    "count"
  ],
  "title": "ReceivingSetDocument",
  "type": "object"
}
```
