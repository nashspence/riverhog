# schemas: CollectionUploadDiscardResultOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploaddiscardresultout:ba77563b66 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-4c548f4342"></a>
- <a id="s-0a15ad2a71"></a>`title`: CollectionUploadDiscardResultOut
- <a id="s-ec25a7020b"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-32e26780ea"></a>`archive_objects` | yes | type="integer" |  |
| <a id="s-84ff4adb17"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-896537db7e"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-c2e32227eb"></a>`custody` | yes | oneOf=#/components/schemas/PendingCollectionUploadCustodyOut \| #/components/schemas/CompleteCollectionUploadCustodyOut; additional keys=`discriminator` |  |
| <a id="s-c5147f11e8"></a>`files` | yes | type="integer"; minimum=0 |  |
| <a id="s-7c66179bc1"></a>`status` | yes | type="string"; enum=["discarded","already_absent"] |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field bytes](#s-84ff4adb17) | `value · schema-value · operational_policy` | shared above |
| [field files](#s-c5147f11e8) | `value · schema-value · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: CompleteCollectionUploadCustodyOut](schemas-completecollectionuploadcustodyout.md)
- [schemas: PendingCollectionUploadCustodyOut](schemas-pendingcollectionuploadcustodyout.md)

## Governing policies

- <a id="pa-d79fbfbf26"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-02b7e5c170"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadDiscardResultOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 77320f97a51d078ec34eacf125437bd66112bf2e14d41b8ff7250a805c1a269b -->

```json
{
  "additionalProperties": false,
  "properties": {
    "archive_objects": {
      "title": "Archive Objects",
      "type": "integer"
    },
    "bytes": {
      "minimum": 0,
      "title": "Bytes",
      "type": "integer"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "custody": {
      "discriminator": {
        "mapping": {
          "complete": "#/components/schemas/CompleteCollectionUploadCustodyOut",
          "pending": "#/components/schemas/PendingCollectionUploadCustodyOut"
        },
        "propertyName": "state"
      },
      "oneOf": [
        {
          "$ref": "#/components/schemas/PendingCollectionUploadCustodyOut"
        },
        {
          "$ref": "#/components/schemas/CompleteCollectionUploadCustodyOut"
        }
      ],
      "title": "Custody"
    },
    "files": {
      "minimum": 0,
      "title": "Files",
      "type": "integer"
    },
    "status": {
      "enum": [
        "discarded",
        "already_absent"
      ],
      "title": "Status",
      "type": "string"
    }
  },
  "required": [
    "status",
    "collection_id",
    "files",
    "bytes",
    "custody",
    "archive_objects"
  ],
  "title": "CollectionUploadDiscardResultOut",
  "type": "object"
}
```
