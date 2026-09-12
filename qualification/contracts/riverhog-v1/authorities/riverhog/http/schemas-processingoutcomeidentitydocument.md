# schemas: ProcessingOutcomeIdentityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingoutcomeidentitydocument:8d55fe5a2a -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingOutcomeIdentityDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CollectionRootIdentityDocument](schemas-collectionrootidentitydocument.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: ProcessingOutcomeIdentityDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `derivation_sha256` | yes | string |  |
| `outcome_id` | yes | string |  |
| `output_collection` | yes | #/components/schemas/CollectionRootIdentityDocument |  |
| `source_claim_id` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9e3282db7bb36155568d93234ae4016c5e043e2c6ac6998425bb60fe3a338509 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "derivation_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Derivation Sha256",
      "type": "string"
    },
    "outcome_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Outcome Id",
      "type": "string"
    },
    "output_collection": {
      "$ref": "#/components/schemas/CollectionRootIdentityDocument"
    },
    "source_claim_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Source Claim Id",
      "type": "string"
    }
  },
  "required": [
    "outcome_id",
    "source_claim_id",
    "output_collection",
    "derivation_sha256"
  ],
  "title": "ProcessingOutcomeIdentityDocument",
  "type": "object"
}
```
