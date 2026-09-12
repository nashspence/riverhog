# schemas: TargetOutputBindingSetIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-targetoutputbindingsetidentity:74f5150368 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetOutputBindingSetIdentity`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: TargetOutputBindingSetIdentity
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `artifact_count` | yes | integer |  |
| `sha256` | yes | string |  |
| `total_bytes` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: daf38f19dc312122935afd84d58bf963fa26e233d8633d1f209196e4f92d8e9a -->

```json
{
  "additionalProperties": false,
  "properties": {
    "artifact_count": {
      "minimum": 1,
      "title": "Artifact Count",
      "type": "integer"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    },
    "total_bytes": {
      "minimum": 0,
      "title": "Total Bytes",
      "type": "integer"
    }
  },
  "required": [
    "artifact_count",
    "total_bytes",
    "sha256"
  ],
  "title": "TargetOutputBindingSetIdentity",
  "type": "object"
}
```
