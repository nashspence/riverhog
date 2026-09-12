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

<a id="s-a4aeb9a49eed"></a>
- <a id="s-15f608b10a7f"></a>`title`: OmittedCollectionProvenanceVerification
- <a id="s-6b4f9d322f93"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9ee420a4ab23"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-ba18f8efc0b8"></a>`entities` | yes | type="integer"; const=0 |  |
| <a id="s-d339222a329e"></a>`files` | yes | type="integer"; minimum=0 |  |
| <a id="s-e4546557e086"></a>`journals` | yes | type="integer"; const=0 |  |
| <a id="s-5721663ce075"></a>`provenance_identity` | yes | type="null" |  |
| <a id="s-25a3b198b325"></a>`provenance_mode` | yes | type="string"; const="omitted" |  |
| <a id="s-05c7f3fde344"></a>`valid` | yes | type="boolean"; const=true |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field files](#s-d339222a329e) | `value · schema-value · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)

## Governing policies

- <a id="pa-cf64061af499"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-cdd626d9569f"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
