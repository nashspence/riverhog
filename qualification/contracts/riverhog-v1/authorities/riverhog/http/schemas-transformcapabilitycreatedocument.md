# schemas: TransformCapabilityCreateDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-transformcapabilitycreatedocument:acd6880541 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/TransformCapabilityCreateDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `contract_max` | maximum=86400, minimum=30, reason=schema-maximum |

## Contract summary

- `title`: TransformCapabilityCreateDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `actions` | no | array |  |
| `audience` | yes | string |  |
| `fence` | yes | integer |  |
| `ttl_seconds` | no | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 017e09367938982b92e94f62bb0ccb5a6249b1f8a8ecf328be4883bbd0b76738 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "actions": {
      "items": {
        "enum": [
          "read-inputs",
          "write-output"
        ],
        "type": "string"
      },
      "minItems": 1,
      "oneOf": [
        {
          "const": [
            "read-inputs"
          ]
        },
        {
          "const": [
            "read-inputs",
            "write-output"
          ]
        }
      ],
      "title": "Actions",
      "type": "array"
    },
    "audience": {
      "pattern": "^[a-z0-9][a-z0-9._:/-]{0,299}$",
      "title": "Audience",
      "type": "string"
    },
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    },
    "ttl_seconds": {
      "default": 900,
      "maximum": 86400,
      "minimum": 30,
      "title": "Ttl Seconds",
      "type": "integer"
    }
  },
  "required": [
    "fence",
    "audience"
  ],
  "title": "TransformCapabilityCreateDocument",
  "type": "object"
}
```
