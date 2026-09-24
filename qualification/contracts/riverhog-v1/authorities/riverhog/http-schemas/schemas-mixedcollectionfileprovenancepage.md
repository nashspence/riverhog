# schemas: MixedCollectionFileProvenancePage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-mixedcollectionfileprovenancepage:6db346ef68 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-f293d0881c"></a>

- <a id="s-b9bf592ddb"></a>`type`: `"object"`
- <a id="s-b50b1f75c7"></a>`additionalProperties`: `false`
- <a id="s-cad3b242a5"></a>`required`: `["page_size","next_page_token","sort","order","query","status","collection_id","provenance_mode","provenance_identity","files"]`
- <a id="s-e5f0a69c1a"></a>`title`: `"MixedCollectionFileProvenancePage"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-062f950653"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-a8aaa69081"></a>`files` | yes | type="array"; items=([_FileProvenanceOut](schemas-fileprovenanceout.md)); title="Files" |  |
| <a id="s-e9387bae1e"></a>`next_page_token` | yes | anyOf=[([BrowsePageToken](schemas-browsepagetoken.md)); (type="null")] |  |
| <a id="s-3d7995e4f3"></a>`order` | yes | [SortOrder](schemas-sortorder.md) |  |
| <a id="s-4aa8fcb191"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100; title="Page Size" |  |
| <a id="s-5407617a49"></a>`provenance_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Provenance Identity" |  |
| <a id="s-c6fe5e265a"></a>`provenance_mode` | yes | type="string"; const="mixed"; title="Provenance Mode" |  |
| <a id="s-a43bcf27e1"></a>`query` | yes | anyOf=[(type="string"); (type="null")]; title="Query" |  |
| <a id="s-2832543499"></a>`sort` | yes | [ProvenanceSort](schemas-provenancesort.md) |  |
| <a id="s-f944118fc7"></a>`status` | yes | anyOf=[([ProvenanceStatus](schemas-provenancestatus.md)); (type="null")] |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field files](#s-a8aaa69081) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-4aa8fcb191) | `value · schema-value · contract_max` | maximum=100; minimum=1; reason="schema-maximum" |
| [field provenance_identity](#s-5407617a49) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract elements

- [BrowsePageToken](schemas-browsepagetoken.md)
- [CollectionId](schemas-collectionid.md)
- [ProvenanceSort](schemas-provenancesort.md)
- [ProvenanceStatus](schemas-provenancestatus.md)
- [SortOrder](schemas-sortorder.md)
- [_FileProvenanceOut](schemas-fileprovenanceout.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-cca877fbd7"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-e7a4a1d642"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-0d5c8b6ea3"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/MixedCollectionFileProvenancePage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0ccef053199f8dc3e65eefb86d70d3f3baa52416018128a30096ed1329eefc36 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "files": {
      "items": {
        "$ref": "#/components/schemas/_FileProvenanceOut"
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
      "pattern": "^[0-9a-f]{64}$",
      "title": "Provenance Identity",
      "type": "string"
    },
    "provenance_mode": {
      "const": "mixed",
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
  "title": "MixedCollectionFileProvenancePage",
  "type": "object"
}
```

</details>
