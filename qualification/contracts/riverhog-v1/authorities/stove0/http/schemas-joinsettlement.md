# schemas: JoinSettlement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-joinsettlement:03537fd1b6 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 6 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/JoinSettlement`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArtifactSelectionRef](schemas-artifactselectionref.md)
- [schemas: CollectionRootRef](schemas-collectionrootref.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: JoinSettlement
- `description`: Success-only, Riverhog-verified result of one resolved join plan.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `derivation_sha256` | yes | string |  |
| `format` | no | string |  |
| `join_plan_sha256` | yes | string |  |
| `output_collection` | yes | #/components/schemas/CollectionRootRef |  |
| `output_selection` | yes | #/components/schemas/ArtifactSelectionRef |  |
| `producer_settlement_sha256` | yes | string |  |
| `settlement_sha256` | yes | string |  |
| `work_id` | yes | string |  |
| `workflow_plan_sha256` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2f55169693f2be5ebba38485523c8687675c1609ec1aa11e9c47c2595c47f11a -->

```json
{
  "additionalProperties": false,
  "description": "Success-only, Riverhog-verified result of one resolved join plan.",
  "properties": {
    "derivation_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Derivation Sha256",
      "type": "string"
    },
    "format": {
      "const": "stove0-join-settlement/v1",
      "default": "stove0-join-settlement/v1",
      "title": "Format",
      "type": "string"
    },
    "join_plan_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Join Plan Sha256",
      "type": "string"
    },
    "output_collection": {
      "$ref": "#/components/schemas/CollectionRootRef"
    },
    "output_selection": {
      "$ref": "#/components/schemas/ArtifactSelectionRef"
    },
    "producer_settlement_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Producer Settlement Sha256",
      "type": "string"
    },
    "settlement_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Settlement Sha256",
      "type": "string"
    },
    "work_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Work Id",
      "type": "string"
    },
    "workflow_plan_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Workflow Plan Sha256",
      "type": "string"
    }
  },
  "required": [
    "work_id",
    "workflow_plan_sha256",
    "join_plan_sha256",
    "derivation_sha256",
    "producer_settlement_sha256",
    "output_collection",
    "output_selection",
    "settlement_sha256"
  ],
  "title": "JoinSettlement",
  "type": "object"
}
```
