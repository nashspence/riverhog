# schemas: ControllerEvidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-controllerevidence:dd1235b71f -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ControllerEvidence`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ExecutionEnvelope](schemas-executionenvelope.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: ControllerEvidence
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `controller_evidence_sha256` | yes | string |  |
| `execution_envelope` | yes | #/components/schemas/ExecutionEnvelope |  |
| `format` | no | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b115148b1c0d4c82c733fe44bf6428461157534079886b60530b9f14c008818e -->

```json
{
  "additionalProperties": false,
  "properties": {
    "controller_evidence_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Controller Evidence Sha256",
      "type": "string"
    },
    "execution_envelope": {
      "$ref": "#/components/schemas/ExecutionEnvelope"
    },
    "format": {
      "const": "stove0-controller-evidence/v1",
      "default": "stove0-controller-evidence/v1",
      "title": "Format",
      "type": "string"
    }
  },
  "required": [
    "execution_envelope",
    "controller_evidence_sha256"
  ],
  "title": "ControllerEvidence",
  "type": "object"
}
```
