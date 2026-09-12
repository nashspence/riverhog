# schemas: TargetSettlementAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-targetsettlementauthority:1928061221 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetSettlementAuthority`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: OutputCollectionRef](schemas-outputcollectionref.md)
- [schemas: TargetOutputBindingSetIdentity](schemas-targetoutputbindingsetidentity.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: TargetSettlementAuthority
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `format` | no | string |  |
| `job_id` | yes | string |  |
| `output_bindings` | yes | #/components/schemas/TargetOutputBindingSetIdentity |  |
| `output_collection` | yes | #/components/schemas/OutputCollectionRef |  |
| `production_sha256` | yes | string |  |
| `settlement_sha256` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a811b1b88f435f8192083bd5bb9cab621322a012a9d17a39beebcbf46f0b2e02 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "format": {
      "const": "stove0-target-settlement/v1",
      "default": "stove0-target-settlement/v1",
      "title": "Format",
      "type": "string"
    },
    "job_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Job Id",
      "type": "string"
    },
    "output_bindings": {
      "$ref": "#/components/schemas/TargetOutputBindingSetIdentity"
    },
    "output_collection": {
      "$ref": "#/components/schemas/OutputCollectionRef"
    },
    "production_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Production Sha256",
      "type": "string"
    },
    "settlement_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Settlement Sha256",
      "type": "string"
    }
  },
  "required": [
    "job_id",
    "production_sha256",
    "output_collection",
    "output_bindings",
    "settlement_sha256"
  ],
  "title": "TargetSettlementAuthority",
  "type": "object"
}
```
