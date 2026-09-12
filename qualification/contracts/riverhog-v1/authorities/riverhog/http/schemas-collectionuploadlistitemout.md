# schemas: CollectionUploadListItemOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadlistitemout:93ae8d69d7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 6 |

## External contract

<a id="s-1a1faf1167"></a>
- <a id="s-d83976a582"></a>`title`: CollectionUploadListItemOut
- <a id="s-843a25a51a"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-78f25613b2"></a>`archive_store` | yes | #/components/schemas/ArchiveStoreName |  |
| <a id="s-9113192326"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-e11197cec6"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-79db77eddc"></a>`created_at` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-75d2cbd26a"></a>`custody` | yes | oneOf=#/components/schemas/PendingCollectionUploadCustodyOut \| #/components/schemas/CompleteCollectionUploadCustodyOut; additional keys=`discriminator` |  |
| <a id="s-59666b97a0"></a>`custody_mode` | yes | type="string"; enum=["producer-retained","custody-transfer"] |  |
| <a id="s-6e8cc7d8e3"></a>`description` | yes | anyOf=#/components/schemas/CollectionDescription \| type="null" |  |
| <a id="s-7e4747dfb6"></a>`description_identity` | yes | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-0ad8095d20"></a>`description_publication` | yes | type="string"; enum=["pending","not_required","current"] |  |
| <a id="s-63ebea617b"></a>`description_revision` | yes | anyOf=type="integer"; minimum=0; maximum=9007199254740991 \| type="null" |  |
| <a id="s-3a8ce2a4f5"></a>`encryption_format` | yes | type="string" |  |
| <a id="s-378db62166"></a>`files` | yes | type="integer"; minimum=0 |  |
| <a id="s-74cdc20906"></a>`ingest_source` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-e51c2fae2e"></a>`orphaned_at` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-335b427411"></a>`passphrase_id` | yes | type="string"; pattern="^[A-Za-z0-9_-]{16,128}$" |  |
| <a id="s-d2a06ec8ff"></a>`state` | yes | type="string"; enum=["open","closing","uploading","finalizing","orphaned","discarding"] |  |
| <a id="s-96988dc3ef"></a>`tag_count` | yes | type="integer"; minimum=0 |  |
| <a id="s-54f0ed2b7c"></a>`tag_publication` | yes | type="string"; enum=["pending","current"] |  |
| <a id="s-3994972441"></a>`tag_revision` | no | anyOf=type="integer"; minimum=1; maximum=9007199254740991 \| type="null" |  |
| <a id="s-6acc291515"></a>`tag_set_identity` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-9142e69595"></a>`upload_state_expires_at` | yes | anyOf=type="string" \| type="null" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field bytes](#s-9113192326) | `value · schema-value · operational_policy` | shared above |
| [field files](#s-378db62166) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-aa83ed105f"></a>[field description_identity · string value](#s-7e4747dfb6) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-2f1d68ce19"></a>[field description_revision · integer value](#s-63ebea617b) | `value · schema-value · contract_max` | maximum=9007199254740991; minimum=0; reason="schema-maximum" |
| <a id="s-c36282bf9d"></a>[field tag_revision · integer value](#s-3994972441) | `value · schema-value · contract_max` | maximum=9007199254740991; minimum=1; reason="schema-maximum" |
| <a id="s-81cb5997db"></a>[field tag_set_identity · string value](#s-6acc291515) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveStoreName](schemas-archivestorename.md)
- [schemas: CollectionDescription](schemas-collectiondescription.md)
- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: CompleteCollectionUploadCustodyOut](schemas-completecollectionuploadcustodyout.md)
- [schemas: PendingCollectionUploadCustodyOut](schemas-pendingcollectionuploadcustodyout.md)

## Governing policies

- <a id="pa-758b538416"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-f4ff05db2e"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-a864548903"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadListItemOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7bf18647f37d5eca74a6b0a83ad41382843e8d818d0e1cd29a032ac5227df2ff -->

```json
{
  "additionalProperties": false,
  "allOf": [
    {
      "if": {
        "properties": {
          "custody_mode": {
            "const": "producer-retained"
          }
        },
        "required": [
          "custody_mode"
        ]
      },
      "then": {
        "properties": {
          "orphaned_at": {
            "type": "null"
          },
          "state": {
            "enum": [
              "open",
              "uploading",
              "finalizing",
              "canceled",
              "finalized"
            ]
          },
          "upload_state_expires_at": {
            "type": "null"
          }
        }
      }
    },
    {
      "if": {
        "properties": {
          "state": {
            "enum": [
              "orphaned",
              "discarding"
            ]
          }
        },
        "required": [
          "state"
        ]
      },
      "then": {
        "properties": {
          "custody_mode": {
            "const": "custody-transfer"
          },
          "orphaned_at": {
            "type": "string"
          },
          "upload_state_expires_at": {
            "type": "null"
          }
        }
      }
    },
    {
      "if": {
        "properties": {
          "custody_mode": {
            "const": "custody-transfer"
          },
          "state": {
            "enum": [
              "open",
              "closing"
            ]
          }
        },
        "required": [
          "custody_mode",
          "state"
        ]
      },
      "then": {
        "properties": {
          "orphaned_at": {
            "type": "null"
          },
          "upload_state_expires_at": {
            "type": "string"
          }
        }
      }
    },
    {
      "if": {
        "properties": {
          "custody_mode": {
            "const": "custody-transfer"
          },
          "state": {
            "enum": [
              "uploading",
              "finalizing",
              "canceled",
              "finalized"
            ]
          }
        },
        "required": [
          "custody_mode",
          "state"
        ]
      },
      "then": {
        "properties": {
          "orphaned_at": {
            "type": "null"
          },
          "upload_state_expires_at": {
            "type": "null"
          }
        }
      }
    },
    {
      "if": {
        "properties": {
          "state": {
            "enum": [
              "finalizing",
              "finalized"
            ]
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
    },
    {
      "if": {
        "properties": {
          "custody_mode": {
            "const": "custody-transfer"
          },
          "state": {
            "const": "uploading"
          }
        },
        "required": [
          "custody_mode",
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
    "archive_store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "bytes": {
      "minimum": 0,
      "title": "Bytes",
      "type": "integer"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "created_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Created At"
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
    "custody_mode": {
      "enum": [
        "producer-retained",
        "custody-transfer"
      ],
      "title": "Custody Mode",
      "type": "string"
    },
    "description": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CollectionDescription"
        },
        {
          "type": "null"
        }
      ]
    },
    "description_identity": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Description Identity"
    },
    "description_publication": {
      "enum": [
        "pending",
        "not_required",
        "current"
      ],
      "title": "Description Publication",
      "type": "string"
    },
    "description_revision": {
      "anyOf": [
        {
          "maximum": 9007199254740991,
          "minimum": 0,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Description Revision"
    },
    "encryption_format": {
      "title": "Encryption Format",
      "type": "string"
    },
    "files": {
      "minimum": 0,
      "title": "Files",
      "type": "integer"
    },
    "ingest_source": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Ingest Source"
    },
    "orphaned_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Orphaned At"
    },
    "passphrase_id": {
      "pattern": "^[A-Za-z0-9_-]{16,128}$",
      "title": "Passphrase Id",
      "type": "string"
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
    "tag_count": {
      "minimum": 0,
      "title": "Tag Count",
      "type": "integer"
    },
    "tag_publication": {
      "enum": [
        "pending",
        "current"
      ],
      "title": "Tag Publication",
      "type": "string"
    },
    "tag_revision": {
      "anyOf": [
        {
          "maximum": 9007199254740991,
          "minimum": 1,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Tag Revision"
    },
    "tag_set_identity": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Tag Set Identity"
    },
    "upload_state_expires_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Upload State Expires At"
    }
  },
  "required": [
    "collection_id",
    "created_at",
    "ingest_source",
    "description",
    "description_revision",
    "description_identity",
    "description_publication",
    "tag_publication",
    "tag_count",
    "archive_store",
    "encryption_format",
    "passphrase_id",
    "state",
    "custody_mode",
    "files",
    "bytes",
    "custody",
    "upload_state_expires_at",
    "orphaned_at"
  ],
  "title": "CollectionUploadListItemOut",
  "type": "object"
}
```
