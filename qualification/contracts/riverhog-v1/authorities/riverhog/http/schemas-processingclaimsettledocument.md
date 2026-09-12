# schemas: ProcessingClaimSettleDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingclaimsettledocument:81a030c9d6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-53b164176d"></a>
- <a id="s-c084ae42f8"></a>`title`: ProcessingClaimSettleDocument
- <a id="s-7cefd7d377"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2c33191341"></a>`derivation` | yes | #/components/schemas/CollectionDerivationDocument |  |
| <a id="s-3353cd3ebf"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-593c65a4f2"></a>`outcome` | no | anyOf=#/components/schemas/ProcessingOutcomeBindingDocument \| type="null" |  |
| <a id="s-bc8bb48614"></a>`output_collection_id` | yes | #/components/schemas/CollectionId |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionDerivationDocument](schemas-collectionderivationdocument.md)
- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: ProcessingOutcomeBindingDocument](schemas-processingoutcomebindingdocument.md)

## Governing policies

- <a id="pa-84a1a2e337"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimSettleDocument`

### Exact owned JSON

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
