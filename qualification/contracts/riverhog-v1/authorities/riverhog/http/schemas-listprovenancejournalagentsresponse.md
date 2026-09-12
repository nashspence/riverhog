# schemas: ListProvenanceJournalAgentsResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-listprovenancejournalagentsresponse:4a3def0a49 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-21a8cfb0d8b5"></a>
- <a id="s-e57dbe70da0a"></a>`title`: ListProvenanceJournalAgentsResponse
- <a id="s-d79bbc264a06"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e9ed861c1837"></a>`agents` | yes | type="array"; items=(#/components/schemas/ProvenanceJournalAgentOut) |  |
| <a id="s-5355e5f6b007"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-1f61ad15dc37"></a>`journal_id` | yes | #/components/schemas/ProvenanceJournalId |  |
| <a id="s-cb4d194207c6"></a>`next_page_token` | yes | anyOf=#/components/schemas/BrowsePageToken \| type="null" |  |
| <a id="s-7b808efc5398"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb21)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field agents](#s-e9ed861c1837) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-7b808efc5398) | `value · schema-value · contract_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: ProvenanceJournalAgentOut](schemas-provenancejournalagentout.md)
- [schemas: ProvenanceJournalId](schemas-provenancejournalid.md)

## Governing policies

- <a id="pa-e44f9b0ee4aa"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-43b326ebdabd"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb21)
- <a id="pa-6839fda22cc1"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ListProvenanceJournalAgentsResponse`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 49714167644d18cabb26c9bef93acc269985c01433f93c17107afa90c22980aa -->

```json
{
  "additionalProperties": false,
  "properties": {
    "agents": {
      "items": {
        "$ref": "#/components/schemas/ProvenanceJournalAgentOut"
      },
      "title": "Agents",
      "type": "array"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "journal_id": {
      "$ref": "#/components/schemas/ProvenanceJournalId"
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
    }
  },
  "required": [
    "collection_id",
    "journal_id",
    "page_size",
    "next_page_token",
    "agents"
  ],
  "title": "ListProvenanceJournalAgentsResponse",
  "type": "object"
}
```
