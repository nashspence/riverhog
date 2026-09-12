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

<a id="s-74e5c2af8169"></a>
- <a id="s-4be1783134dd"></a>`title`: CapturedCollectionProvenanceVerification
- <a id="s-ce742574d3e1"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-87ba9aa6578c"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-43af0c7bb16c"></a>`entities` | yes | type="integer"; minimum=0 |  |
| <a id="s-bb1cbb5ae80d"></a>`files` | yes | type="integer"; minimum=0 |  |
| <a id="s-bbffa50bf8ed"></a>`journals` | yes | type="integer"; minimum=1 |  |
| <a id="s-4e4fa3e79b7b"></a>`provenance_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f9e255b72eff"></a>`provenance_mode` | yes | type="string"; enum=["captured","mixed"] |  |
| <a id="s-2e4362d4dc82"></a>`valid` | yes | type="boolean"; const=true |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field files](#s-bb1cbb5ae80d) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field provenance_identity](#s-4e4fa3e79b7b) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)

## Governing policies

- <a id="pa-fb0e31eb57ef"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-0166e2d19f4e"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)
- <a id="pa-7eaffa80c63f"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
