# schemas: ListProvenanceJournalAgentsResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-listprovenancejournalagentsresponse:4a3def0a49 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ListProvenanceJournalAgentsResponse`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/route-progression/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: ProvenanceJournalAgentOut](schemas-provenancejournalagentout.md)
- [schemas: ProvenanceJournalId](schemas-provenancejournalid.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | reason=bounded-route-page |
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: ListProvenanceJournalAgentsResponse
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `agents` | yes | array |  |
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `journal_id` | yes | #/components/schemas/ProvenanceJournalId |  |
| `next_page_token` | yes | object (1 fields) |  |
| `page_size` | yes | integer |  |

## Complete owned contract

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
