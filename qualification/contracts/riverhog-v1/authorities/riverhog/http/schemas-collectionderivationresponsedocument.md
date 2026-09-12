# schemas: CollectionDerivationResponseDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionderivationresponsedocument:78eea1dbde -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionDerivationResponseDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CollectionDerivationDocument](schemas-collectionderivationdocument.md)
- [schemas: CollectionId](schemas-collectionid.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: CollectionDerivationResponseDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `derivation` | yes | #/components/schemas/CollectionDerivationDocument |  |
| `document_sha256` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 25c1147d5abf6e3c77d52a78af2902e435028ff52078b5db3c35442caee49f58 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "derivation": {
      "$ref": "#/components/schemas/CollectionDerivationDocument"
    },
    "document_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Document Sha256",
      "type": "string"
    }
  },
  "required": [
    "collection_id",
    "document_sha256",
    "derivation"
  ],
  "title": "CollectionDerivationResponseDocument",
  "type": "object"
}
```
