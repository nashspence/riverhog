# schemas: AcceptedTargetJob

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-acceptedtargetjob:571531b8ae -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/AcceptedTargetJob`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: TargetJobDeclaration](schemas-targetjobdeclaration.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: AcceptedTargetJob
- `description`: Durable, non-secret identity of one accepted target job request.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `declaration` | yes | #/components/schemas/TargetJobDeclaration |  |
| `request_sha256` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5f00e108b9ab996f28a1dfd3928c40b9045b480dd5240737de55621c6ceb42bd -->

```json
{
  "additionalProperties": false,
  "description": "Durable, non-secret identity of one accepted target job request.",
  "properties": {
    "declaration": {
      "$ref": "#/components/schemas/TargetJobDeclaration"
    },
    "request_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Request Sha256",
      "type": "string"
    }
  },
  "required": [
    "declaration",
    "request_sha256"
  ],
  "title": "AcceptedTargetJob",
  "type": "object"
}
```
