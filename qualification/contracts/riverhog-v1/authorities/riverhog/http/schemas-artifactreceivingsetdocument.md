# schemas: ArtifactReceivingSetDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-artifactreceivingsetdocument:e09fd4b776 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArtifactReceivingSetDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArtifactSetAuthorityDocument](schemas-artifactsetauthoritydocument.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: ArtifactReceivingSetDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `authority` | no | object (1 fields) |  |
| `count` | yes | integer |  |
| `state` | yes | string |  |
| `total_bytes` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b9a5b3f9d1aeb47fe357397baeee9b1dbeb64055a21618883199489f4789bcbb -->

```json
{
  "additionalProperties": false,
  "properties": {
    "authority": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ArtifactSetAuthorityDocument"
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
    },
    "total_bytes": {
      "minimum": 0,
      "title": "Total Bytes",
      "type": "integer"
    }
  },
  "required": [
    "state",
    "count",
    "total_bytes"
  ],
  "title": "ArtifactReceivingSetDocument",
  "type": "object"
}
```
