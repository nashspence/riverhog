# schemas: ProcessingClaimPlanDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingclaimplandocument:0e187c388f -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimPlanDocument`

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

- [schemas: ArtifactSetAuthorityDocument](schemas-artifactsetauthoritydocument.md)
- [schemas: ExactSetAuthorityDocument](schemas-exactsetauthoritydocument.md)
- [schemas: OperationIdentityDocument](schemas-operationidentitydocument.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| encoded-size | bytes | `contract_max` | maximum=16777216, reason=bounded-controller-evidence-envelope |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=64, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: ProcessingClaimPlanDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `artifacts` | yes | #/components/schemas/ArtifactSetAuthorityDocument |  |
| `controller_evidence` | yes | object |  |
| `controller_evidence_sha256` | yes | string |  |
| `execution_id` | yes | string |  |
| `inputs` | yes | #/components/schemas/ExactSetAuthorityDocument |  |
| `operation` | yes | #/components/schemas/OperationIdentityDocument |  |
| `retirement_grace_seconds` | yes | integer |  |
| `retirement_policy` | yes | string |  |
| `sealed_at` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c782ec1e7e9f82a2050027187b4f010e5c02d15a1518bc4a009c6856fb7dd042 -->

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
    "artifacts": {
      "$ref": "#/components/schemas/ArtifactSetAuthorityDocument"
    },
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
    "inputs": {
      "$ref": "#/components/schemas/ExactSetAuthorityDocument"
    },
    "operation": {
      "$ref": "#/components/schemas/OperationIdentityDocument"
    },
    "retirement_grace_seconds": {
      "minimum": 0,
      "title": "Retirement Grace Seconds",
      "type": "integer"
    },
    "retirement_policy": {
      "enum": [
        "retain",
        "retire-after-verified-output"
      ],
      "title": "Retirement Policy",
      "type": "string"
    },
    "sealed_at": {
      "maxLength": 64,
      "minLength": 1,
      "title": "Sealed At",
      "type": "string"
    }
  },
  "required": [
    "execution_id",
    "controller_evidence",
    "controller_evidence_sha256",
    "operation",
    "inputs",
    "artifacts",
    "retirement_policy",
    "retirement_grace_seconds",
    "sealed_at"
  ],
  "then": {
    "properties": {
      "retirement_grace_seconds": {
        "const": 0
      }
    }
  },
  "title": "ProcessingClaimPlanDocument",
  "type": "object"
}
```
