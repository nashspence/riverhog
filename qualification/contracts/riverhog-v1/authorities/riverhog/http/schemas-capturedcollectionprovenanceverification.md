# schemas: CapturedCollectionProvenanceVerification

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-capturedcollectionprovenanceverification:4ae87f704e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-74e5c2af81"></a>
- <a id="s-4be1783134"></a>`title`: CapturedCollectionProvenanceVerification
- <a id="s-ce742574d3"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-87ba9aa657"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-43af0c7bb1"></a>`entities` | yes | type="integer"; minimum=0 |  |
| <a id="s-bb1cbb5ae8"></a>`files` | yes | type="integer"; minimum=0 |  |
| <a id="s-bbffa50bf8"></a>`journals` | yes | type="integer"; minimum=1 |  |
| <a id="s-4e4fa3e79b"></a>`provenance_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f9e255b72e"></a>`provenance_mode` | yes | type="string"; enum=["captured","mixed"] |  |
| <a id="s-2e4362d4dc"></a>`valid` | yes | type="boolean"; const=true |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field files](#s-bb1cbb5ae8) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field provenance_identity](#s-4e4fa3e79b) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)

## Governing policies

- <a id="pa-fb0e31eb57"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-0166e2d19f"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-7eaffa80c6"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CapturedCollectionProvenanceVerification`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 38a26f65eebc99d4e46a9140941d64befd49c88fc9fd1dba1f2059f05997f2b2 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "entities": {
      "minimum": 0,
      "title": "Entities",
      "type": "integer"
    },
    "files": {
      "minimum": 0,
      "title": "Files",
      "type": "integer"
    },
    "journals": {
      "minimum": 1,
      "title": "Journals",
      "type": "integer"
    },
    "provenance_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Provenance Identity",
      "type": "string"
    },
    "provenance_mode": {
      "enum": [
        "captured",
        "mixed"
      ],
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
  "title": "CapturedCollectionProvenanceVerification",
  "type": "object"
}
```
