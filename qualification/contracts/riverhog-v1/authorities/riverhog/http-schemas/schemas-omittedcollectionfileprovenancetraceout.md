# schemas: OmittedCollectionFileProvenanceTraceOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-omittedcollectionfileprovenancetraceout:7094538b09 -->

Exact externally visible contract owned by this contract element.

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
| <a id="s-090c680ea6"></a>`bytes` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md) |  |
| <a id="s-cadd7e4202"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-fe03a35378"></a>`items` | yes | type="array"; items=([ProvenanceTraceItemOut](schemas-provenancetraceitemout.md)); title="Items" |  |
| <a id="s-9eb0308058"></a>`journal` | no | type="null"; title="Journal" |  |
| <a id="s-17962107fc"></a>`next_page_token` | yes | anyOf=[([BrowsePageToken](schemas-browsepagetoken.md)); (type="null")] |  |
| <a id="s-adc43cc81f"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100; title="Page Size" |  |
| <a id="s-4b217563fd"></a>`path` | yes | [CanonicalRelPath](schemas-canonicalrelpath.md) |  |
| <a id="s-0e08484812"></a>`provenance` | yes | [OmittedFileProvenanceBinding](schemas-omittedfileprovenancebinding.md) |  |
| <a id="s-e7a319e633"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field items](#s-fe03a35378) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-adc43cc81f) | `value · schema-value · contract_max` | maximum=100; minimum=1; reason="schema-maximum" |
| [field sha256](#s-e7a319e633) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract elements

- [BrowsePageToken](schemas-browsepagetoken.md)
- [CanonicalRelPath](schemas-canonicalrelpath.md)
- [CollectionId](schemas-collectionid.md)
- [NonnegativeDecimal](schemas-nonnegativedecimal.md)
- [OmittedFileProvenanceBinding](schemas-omittedfileprovenancebinding.md)
- [ProvenanceTraceItemOut](schemas-provenancetraceitemout.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-39e40742bc"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-3946f75506"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-46a722910b"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/OmittedCollectionFileProvenanceTraceOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eceb121dc36753e9ab2fe8f6b2fae26cfa184a9d64382206a6b6191c234d6926 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "bytes": {
      "$ref": "#/components/schemas/NonnegativeDecimal"
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
