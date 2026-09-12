# schemas: WorkClaimView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-workclaimview:85c81f7ab8 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkClaimView`

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
| length | characters | `contract_max` | maximum=160, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: WorkClaimView
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `claim_id` | yes | string |  |
| `fence` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5836e31fafa2f07e9c5cb14ccd4562b4c1e719d136d58b41e3f193e6c594bc53 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "claim_id": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Claim Id",
      "type": "string"
    },
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    }
  },
  "required": [
    "claim_id",
    "fence"
  ],
  "title": "WorkClaimView",
  "type": "object"
}
```
