# schemas: CollectionDerivationResponseDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionderivationresponsedocument:78eea1dbde -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-5c1e8ccd2b"></a>
- <a id="s-785d799a4c"></a>`title`: CollectionDerivationResponseDocument
- <a id="s-9794cccef5"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6aeb171cd6"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-55e28adc48"></a>`derivation` | yes | #/components/schemas/CollectionDerivationDocument |  |
| <a id="s-fae979dcc8"></a>`document_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field document_sha256](#s-fae979dcc8) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionDerivationDocument](schemas-collectionderivationdocument.md)
- [schemas: CollectionId](schemas-collectionid.md)

## Governing policies

- <a id="pa-b81c1fc7d1"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-10074f8066"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionDerivationResponseDocument`

### Exact owned JSON

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
