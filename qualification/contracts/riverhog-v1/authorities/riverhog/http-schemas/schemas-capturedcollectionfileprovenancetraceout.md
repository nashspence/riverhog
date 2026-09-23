# schemas: CapturedCollectionFileProvenanceTraceOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-capturedcollectionfileprovenancetraceout:91a144a0bb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-8271ebccc8"></a>

- <a id="s-8f2b81060c"></a>`type`: `"object"`
- <a id="s-e6205e293d"></a>`additionalProperties`: `false`
- <a id="s-7fd95e62b9"></a>`required`: `["page_size","next_page_token","items","path","bytes","sha256","collection_id","provenance","journal"]`
- <a id="s-4413335467"></a>`title`: `"CapturedCollectionFileProvenanceTraceOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a5863a315d"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-ce32f13faa"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-6736d431c3"></a>`items` | yes | type="array"; items=([ProvenanceTraceItemOut](schemas-provenancetraceitemout.md)); title="Items" |  |
| <a id="s-4a71d3cab5"></a>`journal` | yes | [ProvenanceJournalOut](schemas-provenancejournalout.md) |  |
| <a id="s-988e1d208f"></a>`next_page_token` | yes | anyOf=[([BrowsePageToken](schemas-browsepagetoken.md)); (type="null")] |  |
| <a id="s-7e35635550"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100; title="Page Size" |  |
| <a id="s-2b50f8202f"></a>`path` | yes | [CanonicalRelPath](schemas-canonicalrelpath.md) |  |
| <a id="s-77b4c1146c"></a>`provenance` | yes | [CapturedFileProvenanceBinding](schemas-capturedfileprovenancebinding.md) |  |
| <a id="s-f8a55e0617"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field bytes](#s-a5863a315d) | `value · schema-value · operational_policy` | shared above |
| [field items](#s-6736d431c3) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-7e35635550) | `value · schema-value · contract_max` | maximum=100; minimum=1; reason="schema-maximum" |
| [field sha256](#s-f8a55e0617) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract elements

- [BrowsePageToken](schemas-browsepagetoken.md)
- [CanonicalRelPath](schemas-canonicalrelpath.md)
- [CapturedFileProvenanceBinding](schemas-capturedfileprovenancebinding.md)
- [CollectionId](schemas-collectionid.md)
- [ProvenanceJournalOut](schemas-provenancejournalout.md)
- [ProvenanceTraceItemOut](schemas-provenancetraceitemout.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-583ca8b156"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-9babb928bc"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-59d5a3c4e7"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CapturedCollectionFileProvenanceTraceOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 26200295e7920d8776fa0775f15e88e74b063ce70d2f3f13ed398e12b98671b2 -->

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
      "$ref": "#/components/schemas/ProvenanceJournalOut"
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
      "$ref": "#/components/schemas/CapturedFileProvenanceBinding"
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
    "provenance",
    "journal"
  ],
  "title": "CapturedCollectionFileProvenanceTraceOut",
  "type": "object"
}
```

</details>
