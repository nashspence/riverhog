# schemas: CollectionUploadDiscardResultOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionuploaddiscardresultout:1304bf4e81 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-4c548f4342"></a>

- <a id="s-ec25a7020b"></a>`type`: `"object"`
- <a id="s-9b2a8abbf7"></a>`additionalProperties`: `false`
- <a id="s-187c82aa3c"></a>`required`: `["status","collection_id","files","bytes","custody","archive_objects"]`
- <a id="s-0a15ad2a71"></a>`title`: `"CollectionUploadDiscardResultOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-32e26780ea"></a>`archive_objects` | yes | type="integer"; title="Archive Objects" |  |
| <a id="s-84ff4adb17"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-896537db7e"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-c2e32227eb"></a>`custody` | yes | discriminator={"mapping":{"complete":"#/components/schemas/CompleteCollectionUploadCustodyOut","pending":"#/components/schemas/PendingCollectionUploadCustodyOut"},"propertyName":"state"}; oneOf=[([PendingCollectionUploadCustodyOut](schemas-pendingcollectionuploadcustodyout.md)); ([CompleteCollectionUploadCustodyOut](schemas-completecollectionuploadcustodyout.md))]; title="Custody" |  |
| <a id="s-c5147f11e8"></a>`files` | yes | type="integer"; minimum=0; title="Files" |  |
| <a id="s-7c66179bc1"></a>`status` | yes | type="string"; enum=["discarded","already_absent"]; title="Status" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field bytes](#s-84ff4adb17) | `value · schema-value · operational_policy` | shared above |
| [field files](#s-c5147f11e8) | `value · schema-value · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract elements

- [CollectionId](schemas-collectionid.md)
- [CompleteCollectionUploadCustodyOut](schemas-completecollectionuploadcustodyout.md)
- [PendingCollectionUploadCustodyOut](schemas-pendingcollectionuploadcustodyout.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-3d9a088edd"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-309366660f"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadDiscardResultOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
