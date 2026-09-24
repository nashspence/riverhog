# schemas: ListProvenanceJournalAgentsOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-listprovenancejournalagentsout:243dd922af -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-075160555b"></a>

- <a id="s-2cd6985303"></a>`type`: `"object"`
- <a id="s-9c9c731840"></a>`additionalProperties`: `false`
- <a id="s-be4daab0c2"></a>`required`: `["collection_id","journal_id","page_size","next_page_token","agents"]`
- <a id="s-516e64b134"></a>`title`: `"ListProvenanceJournalAgentsOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fb3f42ae3a"></a>`agents` | yes | type="array"; items=([ProvenanceJournalAgentOut](schemas-provenancejournalagentout.md)); title="Agents" |  |
| <a id="s-5581dca06e"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-3d10db7781"></a>`journal_id` | yes | [ProvenanceJournalId](schemas-provenancejournalid.md) |  |
| <a id="s-6b783e5aae"></a>`next_page_token` | yes | anyOf=[([BrowsePageToken](schemas-browsepagetoken.md)); (type="null")] |  |
| <a id="s-f140b2678f"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100; title="Page Size" |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field agents](#s-fb3f42ae3a) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-f140b2678f) | `value · schema-value · contract_max` | shared above |

### Evidence gaps

The named contract groups have recorded evidence gaps in the following guarantees. Each group's page identifies its exact open guarantees and candidate tests:

- Each step stays within its declared limits.
- Continuing the work makes progress toward its declared completion.
- The operation works across multiple pages or chunks.
- Required data or work is not silently left out.
- Work can resume after a restart as its contract requires.

These guarantees let large tasks proceed in smaller steps: a limit on one page or chunk must not become a hidden limit on the whole task. Returning a first page correctly does not establish that continuation or recovery works. Capacity may explicitly reject, defer, or throttle work; it must not silently omit work.

Existing tests may establish individual cases. The gaps retain their recorded group-wide scope and do not establish a bug in every linked contract. Completion follows each contract's rules; mutable browsing carries no implied snapshot guarantee.

Required by: [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb).

Exact evidence groups for this contract element:

- [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md)

## Maintained corroboration

### Referenced contract elements

- [BrowsePageToken](schemas-browsepagetoken.md)
- [CollectionId](schemas-collectionid.md)
- [ProvenanceJournalAgentOut](schemas-provenancejournalagentout.md)
- [ProvenanceJournalId](schemas-provenancejournalid.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-befe44c45e"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-20edcb78d0"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)
- <a id="pa-2259abb8b0"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ListProvenanceJournalAgentsOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c9a9da53a49dac7266fc19db5ebe1b8102a1aa0ae70841a29361e25704ebfe18 -->

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
  "title": "ListProvenanceJournalAgentsOut",
  "type": "object"
}
```

</details>
