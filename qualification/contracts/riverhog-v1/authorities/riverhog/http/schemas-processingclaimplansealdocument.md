# schemas: ProcessingClaimPlanSealDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingclaimplansealdocument:c20777cee8 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimPlanSealDocument`

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

- [schemas: OperationIdentityDocument](schemas-operationidentitydocument.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| encoded-size | bytes | `contract_max` | maximum=16777216, reason=bounded-controller-evidence-envelope |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: ProcessingClaimPlanSealDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `controller_evidence` | yes | object |  |
| `controller_evidence_sha256` | yes | string |  |
| `execution_id` | yes | string |  |
| `fence` | yes | integer |  |
| `operation` | yes | #/components/schemas/OperationIdentityDocument |  |
| `retirement_grace_seconds` | no | integer |  |
| `retirement_policy` | no | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 019f4d09832c4a7a45a7a4c9af885f747cd565fca4fa700b5359bf03646121f1 -->

```json
{
  "additionalProperties": false,
  "if": {
    "properties": {
      "retirement_policy": {
        "const": "retain"
      }
    }
  },
  "properties": {
    "controller_evidence": {
      "additionalProperties": true,
      "title": "Controller Evidence",
      "type": "object",
      "x-riverhog-encoded-bytes-max": 16777216,
      "x-riverhog-extent": {
        "policy": "contract_max",
        "reason": "bounded-controller-evidence-envelope"
      }
    },
    "controller_evidence_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Controller Evidence Sha256",
      "type": "string"
    },
    "execution_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Execution Id",
      "type": "string"
    },
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    },
    "operation": {
      "$ref": "#/components/schemas/OperationIdentityDocument"
    },
    "retirement_grace_seconds": {
      "default": 0,
      "minimum": 0,
      "title": "Retirement Grace Seconds",
      "type": "integer"
    },
    "retirement_policy": {
      "default": "retain",
      "enum": [
        "retain",
        "retire-after-verified-output"
      ],
      "title": "Retirement Policy",
      "type": "string"
    }
  },
  "required": [
    "fence",
    "execution_id",
    "controller_evidence",
    "controller_evidence_sha256",
    "operation"
  ],
  "then": {
    "properties": {
      "retirement_grace_seconds": {
        "const": 0
      }
    }
  },
  "title": "ProcessingClaimPlanSealDocument",
  "type": "object"
}
```
