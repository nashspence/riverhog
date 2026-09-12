# schemas: OmittedCollectionProvenanceVerification

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-omittedcollectionprovenanceverification:ec1e40342f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-a4aeb9a49e"></a>
- <a id="s-15f608b10a"></a>`title`: OmittedCollectionProvenanceVerification
- <a id="s-6b4f9d322f"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9ee420a4ab"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-ba18f8efc0"></a>`entities` | yes | type="integer"; const=0 |  |
| <a id="s-d339222a32"></a>`files` | yes | type="integer"; minimum=0 |  |
| <a id="s-e4546557e0"></a>`journals` | yes | type="integer"; const=0 |  |
| <a id="s-5721663ce0"></a>`provenance_identity` | yes | type="null" |  |
| <a id="s-25a3b198b3"></a>`provenance_mode` | yes | type="string"; const="omitted" |  |
| <a id="s-05c7f3fde3"></a>`valid` | yes | type="boolean"; const=true |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field files](#s-d339222a32) | `value · schema-value · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)

## Governing policies

- <a id="pa-cf64061af4"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-cdd626d956"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/OmittedCollectionProvenanceVerification`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 380bb6a46a823168d6eacdf3c7af3bc5a239d61761518db099828c581b756074 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "entities": {
      "const": 0,
      "title": "Entities",
      "type": "integer"
    },
    "files": {
      "minimum": 0,
      "title": "Files",
      "type": "integer"
    },
    "journals": {
      "const": 0,
      "title": "Journals",
      "type": "integer"
    },
    "provenance_identity": {
      "title": "Provenance Identity",
      "type": "null"
    },
    "provenance_mode": {
      "const": "omitted",
      "title": "Provenance Mode",
      "type": "string"
    },
    "valid": {
      "const": true,
      "title": "Valid",
      "type": "boolean"
    }
  },
  "required": [
    "collection_id",
    "valid",
    "files",
    "entities",
    "provenance_mode",
    "provenance_identity",
    "journals"
  ],
  "title": "OmittedCollectionProvenanceVerification",
  "type": "object"
}
```
