# schemas: OmittedCollectionFileProvenanceTraceOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-omittedcollectionfileprovenancetraceout:7094538b09 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-1144bae5ab"></a>

- <a id="s-6fbe1fb8ee"></a>`type`: `"object"`
- <a id="s-b21f1c0b60"></a>`additionalProperties`: `false`
- <a id="s-77c8d22c70"></a>`required`: `["page_size","next_page_token","items","path","bytes","sha256","collection_id","provenance"]`
- <a id="s-d1d7c3ef6b"></a>`title`: `"OmittedCollectionFileProvenanceTraceOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-090c680ea6"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-cadd7e4202"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-fe03a35378"></a>`items` | yes | type="array"; items=(#/components/schemas/ProvenanceTraceItemOut) |  |
| <a id="s-9eb0308058"></a>`journal` | no | type="null" |  |
| <a id="s-17962107fc"></a>`next_page_token` | yes | anyOf=(#/components/schemas/BrowsePageToken) \| (type="null") |  |
| <a id="s-adc43cc81f"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| <a id="s-4b217563fd"></a>`path` | yes | #/components/schemas/CanonicalRelPath |  |
| <a id="s-0e08484812"></a>`provenance` | yes | #/components/schemas/OmittedFileProvenanceBinding |  |
| <a id="s-e7a319e633"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field bytes](#s-090c680ea6) | `value · schema-value · operational_policy` | shared above |
| [field items](#s-fe03a35378) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-adc43cc81f) | `value · schema-value · contract_max` | maximum=100; minimum=1; reason="schema-maximum" |
| [field sha256](#s-e7a319e633) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract dossiers

- [BrowsePageToken](schemas-browsepagetoken.md)
- [CanonicalRelPath](schemas-canonicalrelpath.md)
- [CollectionId](schemas-collectionid.md)
- [OmittedFileProvenanceBinding](schemas-omittedfileprovenancebinding.md)
- [ProvenanceTraceItemOut](schemas-provenancetraceitemout.md)

## Governing policies

- <a id="pa-39e40742bc"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-3946f75506"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-46a722910b"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/OmittedCollectionFileProvenanceTraceOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bc9a52607f8e75504f0de665e0a2cdbf05c0b91dd8118de1cacaa2571f9e176c -->

```json
{
  "additionalProperties": false,
  "properties": {
    "bytes": {
      "minimum": 0,
      "title": "Bytes",
      "type": "integer"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "items": {
      "items": {
        "$ref": "#/components/schemas/ProvenanceTraceItemOut"
      },
      "title": "Items",
      "type": "array"
    },
    "journal": {
      "title": "Journal",
      "type": "null"
    },
    "next_page_token": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/BrowsePageToken"
        },
        {
          "type": "null"
        }
      ]
    },
    "page_size": {
      "maximum": 100,
      "minimum": 1,
      "title": "Page Size",
      "type": "integer"
    },
    "path": {
      "$ref": "#/components/schemas/CanonicalRelPath"
    },
    "provenance": {
      "$ref": "#/components/schemas/OmittedFileProvenanceBinding"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    }
  },
  "required": [
    "page_size",
    "next_page_token",
    "items",
    "path",
    "bytes",
    "sha256",
    "collection_id",
    "provenance"
  ],
  "title": "OmittedCollectionFileProvenanceTraceOut",
  "type": "object"
}
```

</details>
