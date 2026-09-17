# schemas: CapturedCollectionFileProvenanceDetailOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-capturedcollectionfileprovenancedetailout:c7260d88df -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-34fdfcb936"></a>

- <a id="s-065275a858"></a>`type`: `"object"`
- <a id="s-db3be3aa81"></a>`additionalProperties`: `false`
- <a id="s-380515c4d0"></a>`required`: `["path","bytes","sha256","collection_id","provenance","journal"]`
- <a id="s-8f3c5df021"></a>`title`: `"CapturedCollectionFileProvenanceDetailOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4e35d4945f"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-f753228c2b"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-21a2b064cf"></a>`journal` | yes | [ProvenanceJournalOut](schemas-provenancejournalout.md) |  |
| <a id="s-82225b63ac"></a>`path` | yes | [CanonicalRelPath](schemas-canonicalrelpath.md) |  |
| <a id="s-9fde8a0ba5"></a>`provenance` | yes | [CapturedFileProvenanceBinding](schemas-capturedfileprovenancebinding.md) |  |
| <a id="s-8bd379e970"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field bytes](#s-4e35d4945f) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sha256](#s-8bd379e970) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [CanonicalRelPath](schemas-canonicalrelpath.md)
- [CapturedFileProvenanceBinding](schemas-capturedfileprovenancebinding.md)
- [CollectionId](schemas-collectionid.md)
- [ProvenanceJournalOut](schemas-provenancejournalout.md)

## Governing policies

- <a id="pa-b6207a18b5"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-de7c1d8cf5"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-d5398cd8f4"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CapturedCollectionFileProvenanceDetailOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5b19ce109f17b497a6ffbc145afe5e5fa5db7499aa80215a3f4d2c93f72c0370 -->

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
    "journal": {
      "$ref": "#/components/schemas/ProvenanceJournalOut"
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
    "path",
    "bytes",
    "sha256",
    "collection_id",
    "provenance",
    "journal"
  ],
  "title": "CapturedCollectionFileProvenanceDetailOut",
  "type": "object"
}
```

</details>
