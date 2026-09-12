# schemas: ProcessingClaimOutcomeSettlementDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingclaimoutcomesettlementdocument:fb9ea922ec -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: ProcessingClaimOutcomeSettlementDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `outcomes` | yes | #/components/schemas/ExactSetAuthorityDocument |  |
| `retirement_grace_seconds` | yes | type="integer"; minimum=0 |  |
| `retirement_policy` | yes | type="string"; enum=["retain","retire-after-verified-output"] |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ExactSetAuthorityDocument](schemas-exactsetauthoritydocument.md)

## Governing policies

- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimOutcomeSettlementDocument`

### Exact owned JSON

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
