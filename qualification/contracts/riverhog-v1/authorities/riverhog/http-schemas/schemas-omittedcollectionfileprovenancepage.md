# schemas: OmittedCollectionFileProvenancePage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-omittedcollectionfileprovenancepage:bb6dca91d4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-7f7d0e9982"></a>

- <a id="s-3da2c7c5d8"></a>`type`: `"object"`
- <a id="s-589b4246cf"></a>`additionalProperties`: `false`
- <a id="s-95ce8a1491"></a>`required`: `["page_size","next_page_token","sort","order","query","status","collection_id","provenance_mode","provenance_identity","files"]`
- <a id="s-c53f04453f"></a>`title`: `"OmittedCollectionFileProvenancePage"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cfc0052d4d"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-0f29aaf51f"></a>`files` | yes | type="array"; items=([OmittedCollectionFileProvenanceOut](schemas-omittedcollectionfileprovenanceout.md)); title="Files" |  |
| <a id="s-815d243b32"></a>`next_page_token` | yes | anyOf=[([BrowsePageToken](schemas-browsepagetoken.md)); (type="null")] |  |
| <a id="s-9394ddb730"></a>`order` | yes | [SortOrder](schemas-sortorder.md) |  |
| <a id="s-13afc5c593"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100; title="Page Size" |  |
| <a id="s-9f7c693e4f"></a>`provenance_identity` | yes | type="null"; title="Provenance Identity" |  |
| <a id="s-e2a2173982"></a>`provenance_mode` | yes | type="string"; const="omitted"; title="Provenance Mode" |  |
| <a id="s-62388817d0"></a>`query` | yes | anyOf=[(type="string"); (type="null")]; title="Query" |  |
| <a id="s-6546da24d2"></a>`sort` | yes | [ProvenanceSort](schemas-provenancesort.md) |  |
| <a id="s-b4216cdce6"></a>`status` | yes | anyOf=[([ProvenanceStatus](schemas-provenancestatus.md)); (type="null")] |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field files](#s-0f29aaf51f) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-13afc5c593) | `value · schema-value · contract_max` | shared above |

## Maintained corroboration

### Referenced contract elements

- [BrowsePageToken](schemas-browsepagetoken.md)
- [CollectionId](schemas-collectionid.md)
- [OmittedCollectionFileProvenanceOut](schemas-omittedcollectionfileprovenanceout.md)
- [ProvenanceSort](schemas-provenancesort.md)
- [ProvenanceStatus](schemas-provenancestatus.md)
- [SortOrder](schemas-sortorder.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-0918f4a4e1"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-b641032be1"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-668f063b7f"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/OmittedCollectionFileProvenancePage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4e0bcbee438bf78c2cb28fd0b8815533930a2c131df3393a0c28b175d839fcf4 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "files": {
      "items": {
        "$ref": "#/components/schemas/OmittedCollectionFileProvenanceOut"
      },
      "title": "Files",
      "type": "array"
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
    "order": {
      "$ref": "#/components/schemas/SortOrder"
    },
    "page_size": {
      "maximum": 100,
      "minimum": 1,
      "title": "Page Size",
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
    "query": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Query"
    },
    "sort": {
      "$ref": "#/components/schemas/ProvenanceSort"
    },
    "status": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ProvenanceStatus"
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "page_size",
    "next_page_token",
    "sort",
    "order",
    "query",
    "status",
    "collection_id",
    "provenance_mode",
    "provenance_identity",
    "files"
  ],
  "title": "OmittedCollectionFileProvenancePage",
  "type": "object"
}
```

</details>
