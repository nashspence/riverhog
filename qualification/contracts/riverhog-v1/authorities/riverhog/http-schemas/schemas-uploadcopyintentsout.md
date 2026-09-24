# schemas: UploadCopyIntentsOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-uploadcopyintentsout:3085ccdfda -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-1114f230cc"></a>

- <a id="s-b96f217beb"></a>`type`: `"object"`
- <a id="s-7fd05c60a1"></a>`additionalProperties`: `false`
- <a id="s-c133929cda"></a>`required`: `["collection_id","archive_store","use_cache","copy_to","intents"]`
- <a id="s-8c392ba329"></a>`title`: `"UploadCopyIntentsOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-483fc6fed9"></a>`archive_store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |
| <a id="s-d0aea4d97d"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-eb37aab38b"></a>`copy_to` | yes | type="array"; items=([ArchiveStoreName](schemas-archivestorename.md)); title="Copy To" |  |
| <a id="s-a33416b9c7"></a>`intents` | yes | type="array"; items=([UploadCopyIntentOut](schemas-uploadcopyintentout.md)); title="Intents" |  |
| <a id="s-b6b3a8069a"></a>`use_cache` | yes | type="boolean"; title="Use Cache" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field copy_to](#s-eb37aab38b) | `cardinality · items · operational_policy` | shared above |
| [field intents](#s-a33416b9c7) | `cardinality · items · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ArchiveStoreName](schemas-archivestorename.md)
- [CollectionId](schemas-collectionid.md)
- [UploadCopyIntentOut](schemas-uploadcopyintentout.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-057896464c"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-c549c1fff3"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/UploadCopyIntentsOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eb99d198ce37f2cdfc77865731c80e00149d004db5723b834cfeffa6290b49c7 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "archive_store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "copy_to": {
      "items": {
        "$ref": "#/components/schemas/ArchiveStoreName"
      },
      "title": "Copy To",
      "type": "array"
    },
    "intents": {
      "items": {
        "$ref": "#/components/schemas/UploadCopyIntentOut"
      },
      "title": "Intents",
      "type": "array"
    },
    "use_cache": {
      "title": "Use Cache",
      "type": "boolean"
    }
  },
  "required": [
    "collection_id",
    "archive_store",
    "use_cache",
    "copy_to",
    "intents"
  ],
  "title": "UploadCopyIntentsOut",
  "type": "object"
}
```

</details>
