# schemas: CollectionUploadDiscardPlanOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploaddiscardplanout:00717f2bec -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-6ed9cd7952"></a>
- <a id="s-b48465ba3f"></a>`title`: CollectionUploadDiscardPlanOut
- <a id="s-605f5c9eb2"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8d03a26312"></a>`archive_objects` | yes | type="integer" |  |
| <a id="s-71e5eff23e"></a>`blockers` | yes | type="array"; items=(type="string") |  |
| <a id="s-54499c7b79"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-7d087eddae"></a>`challenge` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-bb62e75af0"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-ff4cd61355"></a>`custody` | yes | oneOf=#/components/schemas/PendingCollectionUploadCustodyOut \| #/components/schemas/CompleteCollectionUploadCustodyOut; additional keys=`discriminator` |  |
| <a id="s-4240669b0c"></a>`expires_at` | yes | type="string" |  |
| <a id="s-c89b6e31a6"></a>`files` | yes | type="integer"; minimum=0 |  |
| <a id="s-f06ccf51a0"></a>`state` | yes | type="string"; enum=["open","closing","uploading","finalizing","orphaned","discarding"] |  |
| <a id="s-e9e793215e"></a>`status` | yes | type="string"; enum=["ready","blocked"] |  |
| <a id="s-30f2c19b26"></a>`warning` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field blockers](#s-71e5eff23e) | `cardinality · items · operational_policy` | shared above |
| [field bytes](#s-54499c7b79) | `value · schema-value · operational_policy` | shared above |
| [field files](#s-c89b6e31a6) | `value · schema-value · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: CompleteCollectionUploadCustodyOut](schemas-completecollectionuploadcustodyout.md)
- [schemas: PendingCollectionUploadCustodyOut](schemas-pendingcollectionuploadcustodyout.md)

## Governing policies

- <a id="pa-42a3dbb522"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-046b2aa5f9"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadDiscardPlanOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9ae8fa9763c1acfe4a0bfaaef8e14e81bff0bc9722261d9ac71ab379ba5cd047 -->

```json
{
  "additionalProperties": false,
  "allOf": [
    {
      "if": {
        "properties": {
          "state": {
            "const": "finalizing"
          }
        },
        "required": [
          "state"
        ]
      },
      "then": {
        "properties": {
          "custody": {
            "properties": {
              "state": {
                "const": "complete"
              }
            },
            "required": [
              "state"
            ]
          }
        }
      }
    }
  ],
  "properties": {
    "archive_objects": {
      "title": "Archive Objects",
      "type": "integer"
    },
    "blockers": {
      "items": {
        "type": "string"
      },
      "title": "Blockers",
      "type": "array"
    },
    "bytes": {
      "minimum": 0,
      "title": "Bytes",
      "type": "integer"
    },
    "challenge": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Challenge"
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
    "expires_at": {
      "title": "Expires At",
      "type": "string"
    },
    "files": {
      "minimum": 0,
      "title": "Files",
      "type": "integer"
    },
    "state": {
      "enum": [
        "open",
        "closing",
        "uploading",
        "finalizing",
        "orphaned",
        "discarding"
      ],
      "title": "State",
      "type": "string"
    },
    "status": {
      "enum": [
        "ready",
        "blocked"
      ],
      "title": "Status",
      "type": "string"
    },
    "warning": {
      "title": "Warning",
      "type": "string"
    }
  },
  "required": [
    "status",
    "collection_id",
    "warning",
    "expires_at",
    "challenge",
    "state",
    "files",
    "bytes",
    "custody",
    "archive_objects",
    "blockers"
  ],
  "title": "CollectionUploadDiscardPlanOut",
  "type": "object"
}
```
