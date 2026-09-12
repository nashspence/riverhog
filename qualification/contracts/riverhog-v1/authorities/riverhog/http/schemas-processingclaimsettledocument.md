# schemas: ProcessingClaimSettleDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingclaimsettledocument:81a030c9d6 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimSettleDocument`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CollectionDerivationDocument](schemas-collectionderivationdocument.md)
- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: ProcessingOutcomeBindingDocument](schemas-processingoutcomebindingdocument.md)

## Contract summary

- `title`: ProcessingClaimSettleDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `derivation` | yes | #/components/schemas/CollectionDerivationDocument |  |
| `fence` | yes | integer |  |
| `outcome` | no | object (1 fields) |  |
| `output_collection_id` | yes | #/components/schemas/CollectionId |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f4eaad05bdbec113776443566aec0733a8a2e4ddde45eaf7609d7cad63baebb5 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "derivation": {
      "$ref": "#/components/schemas/CollectionDerivationDocument"
    },
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    },
    "outcome": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ProcessingOutcomeBindingDocument"
        },
        {
          "type": "null"
        }
      ]
    },
    "output_collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    }
  },
  "required": [
    "fence",
    "output_collection_id",
    "derivation"
  ],
  "title": "ProcessingClaimSettleDocument",
  "type": "object"
}
```
