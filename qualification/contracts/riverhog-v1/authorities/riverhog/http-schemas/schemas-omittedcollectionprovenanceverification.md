# schemas: OmittedCollectionProvenanceVerification

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-omittedcollectionprovenanceverification:b1ec8b8ccb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-a4aeb9a49e"></a>

- <a id="s-6b4f9d322f"></a>`type`: `"object"`
- <a id="s-81236190bd"></a>`additionalProperties`: `false`
- <a id="s-d57c4cba3e"></a>`required`: `["collection_id","valid","files","entities","provenance_mode","provenance_identity","journals"]`
- <a id="s-15f608b10a"></a>`title`: `"OmittedCollectionProvenanceVerification"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9ee420a4ab"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-ba18f8efc0"></a>`entities` | yes | type="integer"; const=0; title="Entities" |  |
| <a id="s-d339222a32"></a>`files` | yes | type="integer"; minimum=0; title="Files" |  |
| <a id="s-e4546557e0"></a>`journals` | yes | type="integer"; const=0; title="Journals" |  |
| <a id="s-5721663ce0"></a>`provenance_identity` | yes | type="null"; title="Provenance Identity" |  |
| <a id="s-25a3b198b3"></a>`provenance_mode` | yes | type="string"; const="omitted"; title="Provenance Mode" |  |
| <a id="s-05c7f3fde3"></a>`valid` | yes | type="boolean"; const=true; title="Valid" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field files](#s-d339222a32) | `value · schema-value · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract elements

- [CollectionId](schemas-collectionid.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-a132e41d4c"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-6aa7529fa5"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/OmittedCollectionProvenanceVerification`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
