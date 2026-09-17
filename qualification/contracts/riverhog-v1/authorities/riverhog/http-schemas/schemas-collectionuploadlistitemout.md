# schemas: CollectionUploadListItemOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionuploadlistitemout:44aff86b90 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-1a1faf1167"></a>

- <a id="s-843a25a51a"></a>`type`: `"object"`
- <a id="s-65c949c64f"></a>`additionalProperties`: `false`
- <a id="s-6b8349d0a5"></a>`required`: `["collection_id","created_at","ingest_source","description","description_revision","description_identity","description_publication","tag_publication","tag_count","archive_store","encryption_format","passphrase_id","state","custody_mode","files","bytes","custody","upload_state_expires_at","orphaned_at"]`
- <a id="s-d83976a582"></a>`title`: `"CollectionUploadListItemOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-78f25613b2"></a>`archive_store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |
| <a id="s-9113192326"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-e11197cec6"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-79db77eddc"></a>`created_at` | yes | anyOf=[(type="string"); (type="null")]; title="Created At" |  |
| <a id="s-75d2cbd26a"></a>`custody` | yes | discriminator={"mapping":{"complete":"#/components/schemas/CompleteCollectionUploadCustodyOut","pending":"#/components/schemas/PendingCollectionUploadCustodyOut"},"propertyName":"state"}; oneOf=[([PendingCollectionUploadCustodyOut](schemas-pendingcollectionuploadcustodyout.md)); ([CompleteCollectionUploadCustodyOut](schemas-completecollectionuploadcustodyout.md))]; title="Custody" |  |
| <a id="s-59666b97a0"></a>`custody_mode` | yes | type="string"; enum=["producer-retained","custody-transfer"]; title="Custody Mode" |  |
| <a id="s-6e8cc7d8e3"></a>`description` | yes | anyOf=[([CollectionDescription](schemas-collectiondescription.md)); (type="null")] |  |
| <a id="s-7e4747dfb6"></a>`description_identity` | yes | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Description Identity" |  |
| <a id="s-0ad8095d20"></a>`description_publication` | yes | type="string"; enum=["pending","not_required","current"]; title="Description Publication" |  |
| <a id="s-63ebea617b"></a>`description_revision` | yes | anyOf=[(type="integer"; minimum=0; maximum=9007199254740991); (type="null")]; title="Description Revision" |  |
| <a id="s-3a8ce2a4f5"></a>`encryption_format` | yes | type="string"; title="Encryption Format" |  |
| <a id="s-378db62166"></a>`files` | yes | type="integer"; minimum=0; title="Files" |  |
| <a id="s-74cdc20906"></a>`ingest_source` | yes | anyOf=[(type="string"); (type="null")]; title="Ingest Source" |  |
| <a id="s-e51c2fae2e"></a>`orphaned_at` | yes | anyOf=[(type="string"); (type="null")]; title="Orphaned At" |  |
| <a id="s-335b427411"></a>`passphrase_id` | yes | type="string"; pattern="^[A-Za-z0-9_-]{16,128}$"; title="Passphrase Id" |  |
| <a id="s-d2a06ec8ff"></a>`state` | yes | type="string"; enum=["open","closing","uploading","finalizing","orphaned","discarding"]; title="State" |  |
| <a id="s-96988dc3ef"></a>`tag_count` | yes | type="integer"; minimum=0; title="Tag Count" |  |
| <a id="s-54f0ed2b7c"></a>`tag_publication` | yes | type="string"; enum=["pending","current"]; title="Tag Publication" |  |
| <a id="s-3994972441"></a>`tag_revision` | no | anyOf=[(type="integer"; minimum=1; maximum=9007199254740991); (type="null")]; title="Tag Revision" |  |
| <a id="s-6acc291515"></a>`tag_set_identity` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Tag Set Identity" |  |
| <a id="s-9142e69595"></a>`upload_state_expires_at` | yes | anyOf=[(type="string"); (type="null")]; title="Upload State Expires At" |  |

### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-387ef16562"></a>1 | properties={custody_mode: (const="producer-retained")}; required=["custody_mode"] | properties={orphaned_at: (type="null"); state: (enum=["open","uploading","finalizing","canceled","finalized"]); upload_state_expires_at: (type="null")} | no additional constraint |
| <a id="s-1b6a7a793e"></a>2 | properties={state: (enum=["orphaned","discarding"])}; required=["state"] | properties={custody_mode: (const="custody-transfer"); orphaned_at: (type="string"); upload_state_expires_at: (type="null")} | no additional constraint |
| <a id="s-e2a303eb39"></a>3 | properties={custody_mode: (const="custody-transfer"); state: (enum=["open","closing"])}; required=["custody_mode","state"] | properties={orphaned_at: (type="null"); upload_state_expires_at: (type="string")} | no additional constraint |
| <a id="s-5844fbddb4"></a>4 | properties={custody_mode: (const="custody-transfer"); state: (enum=["uploading","finalizing","canceled","finalized"])}; required=["custody_mode","state"] | properties={orphaned_at: (type="null"); upload_state_expires_at: (type="null")} | no additional constraint |
| <a id="s-6af3f7c6bc"></a>5 | properties={state: (enum=["finalizing","finalized"])}; required=["state"] | properties={custody: (properties={state: (const="complete")}; required=["state"])} | no additional constraint |
| <a id="s-e7dd30dec7"></a>6 | properties={custody_mode: (const="custody-transfer"); state: (const="uploading")}; required=["custody_mode","state"] | properties={custody: (properties={state: (const="complete")}; required=["state"])} | no additional constraint |

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

- [ArchiveStoreName](schemas-archivestorename.md)
- [CollectionDescription](schemas-collectiondescription.md)
- [CollectionId](schemas-collectionid.md)
- [CompleteCollectionUploadCustodyOut](schemas-completecollectionuploadcustodyout.md)
- [PendingCollectionUploadCustodyOut](schemas-pendingcollectionuploadcustodyout.md)

## Governing policies

- <a id="pa-6da5c1ff5c"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-b45767be95"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-32d23f6108"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadListItemOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
