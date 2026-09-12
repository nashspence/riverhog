# schemas: ProcessingClaimOutcomeSettlementDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingclaimoutcomesettlementdocument:fb9ea922ec -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimOutcomeSettlementDocument`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ExactSetAuthorityDocument](schemas-exactsetauthoritydocument.md)

## Contract summary

- `title`: ProcessingClaimOutcomeSettlementDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `outcomes` | yes | #/components/schemas/ExactSetAuthorityDocument |  |
| `retirement_grace_seconds` | yes | integer |  |
| `retirement_policy` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 80797ec5debdb6aa1dc3e41984cbbe33be59aad57827c3211c7c54b55be29e1e -->

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
    "outcomes": {
      "$ref": "#/components/schemas/ExactSetAuthorityDocument"
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
    }
  },
  "required": [
    "outcomes",
    "retirement_policy",
    "retirement_grace_seconds"
  ],
  "then": {
    "properties": {
      "retirement_grace_seconds": {
        "const": 0
      }
    }
  },
  "title": "ProcessingClaimOutcomeSettlementDocument",
  "type": "object"
}
```
