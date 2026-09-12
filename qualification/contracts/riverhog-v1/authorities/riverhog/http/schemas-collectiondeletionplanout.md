# schemas: CollectionDeletionPlanOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectiondeletionplanout:21e8fd2ce3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 6 |

## External contract

<a id="s-b2d6fcbaba"></a>
- <a id="s-c8bc21fc47"></a>`title`: CollectionDeletionPlanOut
- <a id="s-43a4b11280"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f4ff3def0c"></a>`archive_copies` | yes | type="array"; items=(#/components/schemas/CollectionDeletionArchiveCopyOut) |  |
| <a id="s-0781b71d79"></a>`archive_object_count` | yes | type="integer" |  |
| <a id="s-003b0a0272"></a>`billing_note` | yes | type="string" |  |
| <a id="s-327ac9fe43"></a>`blockers` | yes | type="array"; maxItems=55; items=(type="string"); additional keys=`x-riverhog-extent` |  |
| <a id="s-c328f97416"></a>`bytes` | yes | type="integer" |  |
| <a id="s-3c2ff39367"></a>`challenge` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-73ba2d2bee"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-f0db1aa2f2"></a>`expires_at` | yes | type="string" |  |
| <a id="s-4551e18b33"></a>`file_count` | yes | type="integer" |  |
| <a id="s-0c27dfe4e3"></a>`inventory_identity` | yes | type="string" |  |
| <a id="s-ce48510009"></a>`metadata_rows` | yes | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-137b210b76"></a>`remote_storage_bytes` | yes | type="integer" |  |
| <a id="s-6ac0cd51f1"></a>`retirement_claim` | no | anyOf=#/components/schemas/RetirementClaimReferenceDocument \| type="null" |  |
| <a id="s-dfa08a8c5d"></a>`status` | yes | type="string"; enum=["ready","blocked","deleting"] |  |
| <a id="s-0d4f73bd55"></a>`upload_file_count` | yes | type="integer" |  |
| <a id="s-653b88a8cb"></a>`warning` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-004e25716a"></a>[oneOf alternative 1 · field blockers](#s-b2d6fcbaba) | `cardinality · items · operational_policy` | shared above |
| [field archive_copies](#s-f4ff3def0c) | `cardinality · items · operational_policy` | shared above |
| [field bytes](#s-c328f97416) | `value · schema-value · operational_policy` | shared above |
| [field metadata_rows](#s-ce48510009) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-ae6d9ad024"></a>[oneOf alternative 2 · field blockers](#s-b2d6fcbaba) | `cardinality · items · contract_max` | maximum=0; reason="state-conditioned-empty-set" |
| [field blockers](#s-327ac9fe43) | `cardinality · items · contract_max` | maximum=55; reason="bounded-diagnostic-sample-with-explicit-overflow-markers" |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionDeletionArchiveCopyOut](schemas-collectiondeletionarchivecopyout.md)
- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: RetirementClaimReferenceDocument](schemas-retirementclaimreferencedocument.md)

## Governing policies

- <a id="pa-be125116c2"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-18034768a2"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-aec7605607"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionDeletionPlanOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c45b1e900ac654c02b79b444cd1069c7afa157fcd5e00f8632f6b6b9c6adb342 -->

```json
{
  "additionalProperties": false,
  "oneOf": [
    {
      "properties": {
        "blockers": {
          "minItems": 1
        },
        "challenge": {
          "type": "null"
        },
        "status": {
          "const": "blocked"
        }
      }
    },
    {
      "properties": {
        "blockers": {
          "maxItems": 0
        },
        "challenge": {
          "minLength": 1,
          "type": "string"
        },
        "status": {
          "enum": [
            "ready",
            "deleting"
          ]
        }
      }
    }
  ],
  "properties": {
    "archive_copies": {
      "items": {
        "$ref": "#/components/schemas/CollectionDeletionArchiveCopyOut"
      },
      "title": "Archive Copies",
      "type": "array"
    },
    "archive_object_count": {
      "title": "Archive Object Count",
      "type": "integer"
    },
    "billing_note": {
      "title": "Billing Note",
      "type": "string"
    },
    "blockers": {
      "items": {
        "type": "string"
      },
      "maxItems": 55,
      "title": "Blockers",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "contract_max",
        "reason": "bounded-diagnostic-sample-with-explicit-overflow-markers"
      }
    },
    "bytes": {
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
    "expires_at": {
      "title": "Expires At",
      "type": "string"
    },
    "file_count": {
      "title": "File Count",
      "type": "integer"
    },
    "inventory_identity": {
      "title": "Inventory Identity",
      "type": "string"
    },
    "metadata_rows": {
      "additionalProperties": {
        "type": "integer"
      },
      "title": "Metadata Rows",
      "type": "object"
    },
    "remote_storage_bytes": {
      "title": "Remote Storage Bytes",
      "type": "integer"
    },
    "retirement_claim": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/RetirementClaimReferenceDocument"
        },
        {
          "type": "null"
        }
      ]
    },
    "status": {
      "enum": [
        "ready",
        "blocked",
        "deleting"
      ],
      "title": "Status",
      "type": "string"
    },
    "upload_file_count": {
      "title": "Upload File Count",
      "type": "integer"
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
    "file_count",
    "bytes",
    "archive_copies",
    "archive_object_count",
    "remote_storage_bytes",
    "upload_file_count",
    "inventory_identity",
    "metadata_rows",
    "blockers",
    "billing_note"
  ],
  "title": "CollectionDeletionPlanOut",
  "type": "object"
}
```
