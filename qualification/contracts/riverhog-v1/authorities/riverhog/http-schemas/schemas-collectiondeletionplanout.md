# schemas: CollectionDeletionPlanOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectiondeletionplanout:b8a4c9c7cd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-b2d6fcbaba"></a>

- <a id="s-43a4b11280"></a>`type`: `"object"`
- <a id="s-5dcfb1f38d"></a>`additionalProperties`: `false`
- <a id="s-4195a67cf2"></a>`required`: `["status","collection_id","warning","expires_at","challenge","file_count","bytes","archive_copies","archive_object_count","remote_storage_bytes","upload_file_count","inventory_identity","metadata_rows","blockers","billing_note"]`
- <a id="s-c8bc21fc47"></a>`title`: `"CollectionDeletionPlanOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f4ff3def0c"></a>`archive_copies` | yes | type="array"; items=([CollectionDeletionArchiveCopyOut](schemas-collectiondeletionarchivecopyout.md)); title="Archive Copies" |  |
| <a id="s-0781b71d79"></a>`archive_object_count` | yes | type="integer"; title="Archive Object Count" |  |
| <a id="s-003b0a0272"></a>`billing_note` | yes | type="string"; title="Billing Note" |  |
| <a id="s-327ac9fe43"></a>`blockers` | yes | type="array"; items=(type="string"); maxItems=55; title="Blockers"; x-riverhog-extent={"policy":"contract_max","reason":"bounded-diagnostic-sample-with-explicit-overflow-markers"} |  |
| <a id="s-c328f97416"></a>`bytes` | yes | type="integer"; title="Bytes" |  |
| <a id="s-3c2ff39367"></a>`challenge` | yes | anyOf=[(type="string"); (type="null")]; title="Challenge" |  |
| <a id="s-73ba2d2bee"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-f0db1aa2f2"></a>`expires_at` | yes | type="string"; title="Expires At" |  |
| <a id="s-4551e18b33"></a>`file_count` | yes | type="integer"; title="File Count" |  |
| <a id="s-0c27dfe4e3"></a>`inventory_identity` | yes | type="string"; title="Inventory Identity" |  |
| <a id="s-ce48510009"></a>`metadata_rows` | yes | type="object"; additionalProperties=(type="integer"); title="Metadata Rows" |  |
| <a id="s-137b210b76"></a>`remote_storage_bytes` | yes | type="integer"; title="Remote Storage Bytes" |  |
| <a id="s-6ac0cd51f1"></a>`retirement_claim` | no | anyOf=[([RetirementClaimReferenceDocument](schemas-retirementclaimreferencedocument.md)); (type="null")] |  |
| <a id="s-dfa08a8c5d"></a>`status` | yes | type="string"; enum=["ready","blocked","deleting"]; title="Status" |  |
| <a id="s-0d4f73bd55"></a>`upload_file_count` | yes | type="integer"; title="Upload File Count" |  |
| <a id="s-653b88a8cb"></a>`warning` | yes | type="string"; title="Warning" |  |

### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| 1 | [See `oneOf` alternative 1](#s-f50881f85e) |
| 2 | [See `oneOf` alternative 2](#s-1d57bfa46c) |

### <a id="s-f50881f85e"></a>`oneOf` alternative 1


#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-004e25716a"></a>`blockers` | no | minItems=1 |  |
| <a id="s-b9e2d46f48"></a>`challenge` | no | type="null" |  |
| <a id="s-455f607b2f"></a>`status` | no | const="blocked" |  |

### <a id="s-1d57bfa46c"></a>`oneOf` alternative 2


#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ae6d9ad024"></a>`blockers` | no | maxItems=0 |  |
| <a id="s-809c227b73"></a>`challenge` | no | type="string"; minLength=1 |  |
| <a id="s-792a4c7bd0"></a>`status` | no | enum=["ready","deleting"] |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [oneOf alternative 1 · field blockers](#s-004e25716a) | `cardinality · items · operational_policy` | shared above |
| [field archive_copies](#s-f4ff3def0c) | `cardinality · items · operational_policy` | shared above |
| [field bytes](#s-c328f97416) | `value · schema-value · operational_policy` | shared above |
| [field metadata_rows](#s-ce48510009) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [oneOf alternative 2 · field blockers](#s-ae6d9ad024) | `cardinality · items · contract_max` | maximum=0; reason="state-conditioned-empty-set" |
| [field blockers](#s-327ac9fe43) | `cardinality · items · contract_max` | maximum=55; reason="bounded-diagnostic-sample-with-explicit-overflow-markers" |

## Maintained corroboration

### Referenced contract dossiers

- [CollectionDeletionArchiveCopyOut](schemas-collectiondeletionarchivecopyout.md)
- [CollectionId](schemas-collectionid.md)
- [RetirementClaimReferenceDocument](schemas-retirementclaimreferencedocument.md)

## Governing policies

- <a id="pa-6f7a77a0a1"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-2d403bccd8"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-63bd81c072"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionDeletionPlanOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
