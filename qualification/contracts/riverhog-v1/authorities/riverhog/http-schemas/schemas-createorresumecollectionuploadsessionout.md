# schemas: CreateOrResumeCollectionUploadSessionOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-createorresumecollectionuploadsessionout:d3feb1205a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-0f7d12515a"></a>

- <a id="s-3432f369ea"></a>`type`: `"object"`
- <a id="s-52602bd78e"></a>`additionalProperties`: `false`
- <a id="s-3a428c6e45"></a>`required`: `["collection_id","created_at","ingest_source","description","description_revision","description_identity","description_publication","tag_publication","tag_count","provenance_mode","archive_store","encryption_format","passphrase_id","state","custody_mode","registration_constraints","files_total","bytes_total","upload_state_expires_at","custody","orphaned_at","latest_failure","archive_phase","archive_phase_updated_at","archive_next_attempt_at","collection","resumed"]`
- <a id="s-97c06b7483"></a>`title`: `"CreateOrResumeCollectionUploadSessionOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-593c3aa8bb"></a>`archive_next_attempt_at` | yes | anyOf=[(type="string"; pattern="^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{6}Z$"); (type="null")]; title="Archive Next Attempt At" |  |
| <a id="s-364d7da064"></a>`archive_phase` | yes | type="string"; enum=["planning","uploading","finalization_queued","finalizing","retry_wait","completed","canceled","orphaned","discarding"]; title="Archive Phase" |  |
| <a id="s-1b21051159"></a>`archive_phase_updated_at` | yes | type="string"; pattern="^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{6}Z$"; title="Archive Phase Updated At" |  |
| <a id="s-75de08a13f"></a>`archive_root_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Archive Root Sha256" |  |
| <a id="s-0a9a5c61fb"></a>`archive_storage_prefix` | no | anyOf=[(type="string"); (type="null")]; title="Archive Storage Prefix" |  |
| <a id="s-a661ebc979"></a>`archive_store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |
| <a id="s-7514ed7a31"></a>`archive_total_bytes` | no | anyOf=[(type="integer"); (type="null")]; title="Archive Total Bytes" |  |
| <a id="s-418d1f65fe"></a>`archive_total_units` | no | anyOf=[(type="integer"); (type="null")]; title="Archive Total Units" |  |
| <a id="s-cd6f834e7d"></a>`archive_uploaded_bytes` | no | anyOf=[(type="integer"); (type="null")]; title="Archive Uploaded Bytes" |  |
| <a id="s-695974297a"></a>`archive_uploaded_units` | no | anyOf=[(type="integer"); (type="null")]; title="Archive Uploaded Units" |  |
| <a id="s-1b1fad79d5"></a>`bytes_total` | yes | type="integer"; minimum=0; title="Bytes Total" |  |
| <a id="s-e279eb3f57"></a>`collection` | yes | anyOf=[([CollectionSummaryOut](schemas-collectionsummaryout.md)); (type="null")] |  |
| <a id="s-5aecbbf4c0"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-1337f069e3"></a>`content_identity` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Content Identity" |  |
| <a id="s-e03e99fa8a"></a>`created_at` | yes | type="string"; title="Created At" |  |
| <a id="s-76be39688f"></a>`custody` | yes | discriminator={"mapping":{"complete":"#/components/schemas/CompleteCollectionUploadCustodyOut","pending":"#/components/schemas/PendingCollectionUploadCustodyOut"},"propertyName":"state"}; oneOf=[([PendingCollectionUploadCustodyOut](schemas-pendingcollectionuploadcustodyout.md)); ([CompleteCollectionUploadCustodyOut](schemas-completecollectionuploadcustodyout.md))]; title="Custody" |  |
| <a id="s-a4ebd8e964"></a>`custody_mode` | yes | type="string"; enum=["producer-retained","custody-transfer"]; title="Custody Mode" |  |
| <a id="s-9dd0e9d0dc"></a>`description` | yes | anyOf=[([CollectionDescription](schemas-collectiondescription.md)); (type="null")] |  |
| <a id="s-87279bec9a"></a>`description_identity` | yes | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Description Identity" |  |
| <a id="s-511120fcf8"></a>`description_publication` | yes | type="string"; enum=["pending","not_required","current"]; title="Description Publication" |  |
| <a id="s-97818911b3"></a>`description_revision` | yes | anyOf=[(type="integer"; minimum=0; maximum=9007199254740991); (type="null")]; title="Description Revision" |  |
| <a id="s-a584cdc872"></a>`encryption_format` | yes | type="string"; title="Encryption Format" |  |
| <a id="s-9d1ecaccd3"></a>`files_total` | yes | type="integer"; minimum=0; title="Files Total" |  |
| <a id="s-afe732efdf"></a>`ingest_source` | yes | anyOf=[(type="string"); (type="null")]; title="Ingest Source" |  |
| <a id="s-570ad9d89c"></a>`latest_failure` | yes | anyOf=[(type="string"; maxLength=1000; minLength=1); (type="null")]; title="Latest Failure" |  |
| <a id="s-4158d3e07e"></a>`orphaned_at` | yes | anyOf=[(type="string"); (type="null")]; title="Orphaned At" |  |
| <a id="s-0507afa7fd"></a>`passphrase_id` | yes | type="string"; pattern="^[A-Za-z0-9_-]{16,128}$"; title="Passphrase Id" |  |
| <a id="s-703cd5df49"></a>`provenance_identity` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Provenance Identity" |  |
| <a id="s-7328fa36e5"></a>`provenance_mode` | yes | type="string"; enum=["captured","mixed","omitted"]; title="Provenance Mode" |  |
| <a id="s-1c381e8d2b"></a>`registration_constraints` | yes | anyOf=[([CollectionUploadRegistrationConstraintsOut](schemas-collectionuploadregistrationconstraintsout.md)); (type="null")] |  |
| <a id="s-9af23207b3"></a>`resumed` | yes | type="boolean"; title="Resumed" |  |
| <a id="s-bd565bea65"></a>`state` | yes | type="string"; enum=["open","closing","uploading","finalizing","finalized","canceled","orphaned","discarding"]; title="State" |  |
| <a id="s-3b36552bd1"></a>`tag_count` | yes | type="integer"; minimum=0; title="Tag Count" |  |
| <a id="s-107736f50c"></a>`tag_publication` | yes | type="string"; enum=["pending","current"]; title="Tag Publication" |  |
| <a id="s-77565b278a"></a>`tag_revision` | no | anyOf=[(type="integer"; minimum=1; maximum=9007199254740991); (type="null")]; title="Tag Revision" |  |
| <a id="s-add3845109"></a>`tag_set_identity` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Tag Set Identity" |  |
| <a id="s-c06f1bbb1e"></a>`upload_state_expires_at` | yes | anyOf=[(type="string"); (type="null")]; title="Upload State Expires At" |  |

### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-2b59b74f72"></a>1 | properties={state: (const="finalized")} | properties={archive_root_sha256: (type="string"); collection: (type="object"); content_identity: (type="string"); registration_constraints: (type="null"); tag_publication: (const="current"); tag_revision: (type="integer"); tag_set_identity: (type="string")} | properties={archive_root_sha256: (type="null"); collection: (type="null"); content_identity: (type="null"); registration_constraints: (type="object")} |
| <a id="s-309e598180"></a>2 | properties={tag_publication: (const="current")} | properties={tag_revision: (type="integer"); tag_set_identity: (type="string")} | no additional constraint |
| <a id="s-ec18fdfbd4"></a>3 | properties={state: (const="finalized")}; required=["state"] | oneOf=[(properties={provenance_identity: (type="string"; pattern="^[0-9a-f]{64}$"); provenance_mode: (enum=["captured","mixed"])}; required=["provenance_mode","provenance_identity"]); (properties={provenance_identity: (type="null"); provenance_mode: (const="omitted")}; required=["provenance_mode","provenance_identity"])] | properties={provenance_identity: (type="null"); provenance_mode: (enum=["captured","omitted"])} |
| <a id="s-c3138ea66e"></a>4 | properties={custody_mode: (const="producer-retained")}; required=["custody_mode"] | properties={orphaned_at: (type="null"); state: (enum=["open","uploading","finalizing","canceled","finalized"]); upload_state_expires_at: (type="null")} | no additional constraint |
| <a id="s-c1b4c70eb8"></a>5 | properties={state: (enum=["orphaned","discarding"])}; required=["state"] | properties={custody_mode: (const="custody-transfer"); orphaned_at: (type="string"); upload_state_expires_at: (type="null")} | no additional constraint |
| <a id="s-1d4f93c672"></a>6 | properties={custody_mode: (const="custody-transfer"); state: (enum=["open","closing"])}; required=["custody_mode","state"] | properties={orphaned_at: (type="null"); upload_state_expires_at: (type="string")} | no additional constraint |
| <a id="s-f45e83296e"></a>7 | properties={custody_mode: (const="custody-transfer"); state: (enum=["uploading","finalizing","canceled","finalized"])}; required=["custody_mode","state"] | properties={orphaned_at: (type="null"); upload_state_expires_at: (type="null")} | no additional constraint |
| <a id="s-637bdbe699"></a>8 | properties={state: (enum=["finalizing","finalized"])}; required=["state"] | properties={custody: (properties={state: (const="complete")}; required=["state"])} | no additional constraint |
| <a id="s-7073b57fc3"></a>9 | properties={custody_mode: (const="custody-transfer"); state: (const="uploading")}; required=["custody_mode","state"] | properties={custody: (properties={state: (const="complete")}; required=["state"])} | no additional constraint |
| <a id="s-7c1413c11e"></a>10 | properties={state: (const="open")}; required=["state"] | properties={archive_phase: (enum=["planning","uploading"])}; required=["archive_phase"] | no additional constraint |
| <a id="s-0a11b5467e"></a>11 | properties={state: (const="closing")}; required=["state"] | properties={archive_phase: (enum=["uploading"])}; required=["archive_phase"] | no additional constraint |
| <a id="s-9d941cde9c"></a>12 | properties={state: (const="uploading")}; required=["state"] | properties={archive_phase: (enum=["uploading"])}; required=["archive_phase"] | no additional constraint |
| <a id="s-0534d0cb6f"></a>13 | properties={state: (const="finalizing")}; required=["state"] | properties={archive_phase: (enum=["finalization_queued","finalizing","retry_wait"])}; required=["archive_phase"] | no additional constraint |
| <a id="s-1283629c76"></a>14 | properties={state: (const="finalized")}; required=["state"] | properties={archive_phase: (enum=["completed"])}; required=["archive_phase"] | no additional constraint |
| <a id="s-7696c01c30"></a>15 | properties={state: (const="canceled")}; required=["state"] | properties={archive_phase: (enum=["canceled"])}; required=["archive_phase"] | no additional constraint |
| <a id="s-165c553eda"></a>16 | properties={state: (const="orphaned")}; required=["state"] | properties={archive_phase: (enum=["orphaned"])}; required=["archive_phase"] | no additional constraint |
| <a id="s-69994740cc"></a>17 | properties={state: (const="discarding")}; required=["state"] | properties={archive_phase: (enum=["discarding"])}; required=["archive_phase"] | no additional constraint |
| <a id="s-aa58c3fc68"></a>18 | properties={archive_phase: (const="retry_wait")}; required=["archive_phase"] | properties={archive_next_attempt_at: (type="string"; minLength=1); latest_failure: (type="string"; minLength=1)} | no additional constraint |
| <a id="s-4de8540fd5"></a>19 | properties={archive_phase: (const="finalization_queued")}; required=["archive_phase"] | properties={archive_next_attempt_at: (type="string"; minLength=1); latest_failure: (type="null")} | no additional constraint |
| <a id="s-161e632e2e"></a>20 | properties={archive_phase: (not=(enum=["finalization_queued","retry_wait"]))}; required=["archive_phase"] | properties={archive_next_attempt_at: (type="null")} | no additional constraint |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-aec0b595cc"></a>[allOf alternative 3 · then · oneOf alternative 1 · field provenance_identity](#s-ec18fdfbd4) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-4dac66a09c"></a>[field archive_root_sha256 · string value](#s-75de08a13f) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-0bff60cfb3"></a>[field content_identity · string value](#s-1337f069e3) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-4f378691a3"></a>[field description_identity · string value](#s-87279bec9a) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-975e5196b4"></a>[field description_revision · integer value](#s-97818911b3) | `value · schema-value · contract_max` | maximum=9007199254740991; minimum=0; reason="schema-maximum" |
| <a id="s-645d2232be"></a>[field latest_failure · string value](#s-570ad9d89c) | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |
| <a id="s-1e5f76f5f3"></a>[field provenance_identity · string value](#s-703cd5df49) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-5349b464e2"></a>[field tag_revision · integer value](#s-77565b278a) | `value · schema-value · contract_max` | maximum=9007199254740991; minimum=1; reason="schema-maximum" |
| <a id="s-959a59821a"></a>[field tag_set_identity · string value](#s-add3845109) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract elements

- [ArchiveStoreName](schemas-archivestorename.md)
- [CollectionDescription](schemas-collectiondescription.md)
- [CollectionId](schemas-collectionid.md)
- [CollectionSummaryOut](schemas-collectionsummaryout.md)
- [CollectionUploadRegistrationConstraintsOut](schemas-collectionuploadregistrationconstraintsout.md)
- [CompleteCollectionUploadCustodyOut](schemas-completecollectionuploadcustodyout.md)
- [PendingCollectionUploadCustodyOut](schemas-pendingcollectionuploadcustodyout.md)

## Governing policies

- <a id="pa-1a0180417b"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-1492cd155f"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CreateOrResumeCollectionUploadSessionOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2a1c6d4a2d6413272bc05ba49aa8ce33aa9ca6977b3058b622b3ab37133baccc -->

```json
{
  "additionalProperties": false,
  "allOf": [
    {
      "else": {
        "properties": {
          "archive_root_sha256": {
            "type": "null"
          },
          "collection": {
            "type": "null"
          },
          "content_identity": {
            "type": "null"
          },
          "registration_constraints": {
            "type": "object"
          }
        }
      },
      "if": {
        "properties": {
          "state": {
            "const": "finalized"
          }
        }
      },
      "then": {
        "properties": {
          "archive_root_sha256": {
            "type": "string"
          },
          "collection": {
            "type": "object"
          },
          "content_identity": {
            "type": "string"
          },
          "registration_constraints": {
            "type": "null"
          },
          "tag_publication": {
            "const": "current"
          },
          "tag_revision": {
            "type": "integer"
          },
          "tag_set_identity": {
            "type": "string"
          }
        }
      }
    },
    {
      "if": {
        "properties": {
          "tag_publication": {
            "const": "current"
          }
        }
      },
      "then": {
        "properties": {
          "tag_revision": {
            "type": "integer"
          },
          "tag_set_identity": {
            "type": "string"
          }
        }
      }
    },
    {
      "else": {
        "properties": {
          "provenance_identity": {
            "type": "null"
          },
          "provenance_mode": {
            "enum": [
              "captured",
              "omitted"
            ]
          }
        }
      },
      "if": {
        "properties": {
          "state": {
            "const": "finalized"
          }
        },
        "required": [
          "state"
        ]
      },
      "then": {
        "oneOf": [
          {
            "properties": {
              "provenance_identity": {
                "pattern": "^[0-9a-f]{64}$",
                "type": "string"
              },
              "provenance_mode": {
                "enum": [
                  "captured",
                  "mixed"
                ]
              }
            },
            "required": [
              "provenance_mode",
              "provenance_identity"
            ]
          },
          {
            "properties": {
              "provenance_identity": {
                "type": "null"
              },
              "provenance_mode": {
                "const": "omitted"
              }
            },
            "required": [
              "provenance_mode",
              "provenance_identity"
            ]
          }
        ]
      }
    },
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
    },
    {
      "if": {
        "properties": {
          "state": {
            "const": "open"
          }
        },
        "required": [
          "state"
        ]
      },
      "then": {
        "properties": {
          "archive_phase": {
            "enum": [
              "planning",
              "uploading"
            ]
          }
        },
        "required": [
          "archive_phase"
        ]
      }
    },
    {
      "if": {
        "properties": {
          "state": {
            "const": "closing"
          }
        },
        "required": [
          "state"
        ]
      },
      "then": {
        "properties": {
          "archive_phase": {
            "enum": [
              "uploading"
            ]
          }
        },
        "required": [
          "archive_phase"
        ]
      }
    },
    {
      "if": {
        "properties": {
          "state": {
            "const": "uploading"
          }
        },
        "required": [
          "state"
        ]
      },
      "then": {
        "properties": {
          "archive_phase": {
            "enum": [
              "uploading"
            ]
          }
        },
        "required": [
          "archive_phase"
        ]
      }
    },
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
          "archive_phase": {
            "enum": [
              "finalization_queued",
              "finalizing",
              "retry_wait"
            ]
          }
        },
        "required": [
          "archive_phase"
        ]
      }
    },
    {
      "if": {
        "properties": {
          "state": {
            "const": "finalized"
          }
        },
        "required": [
          "state"
        ]
      },
      "then": {
        "properties": {
          "archive_phase": {
            "enum": [
              "completed"
            ]
          }
        },
        "required": [
          "archive_phase"
        ]
      }
    },
    {
      "if": {
        "properties": {
          "state": {
            "const": "canceled"
          }
        },
        "required": [
          "state"
        ]
      },
      "then": {
        "properties": {
          "archive_phase": {
            "enum": [
              "canceled"
            ]
          }
        },
        "required": [
          "archive_phase"
        ]
      }
    },
    {
      "if": {
        "properties": {
          "state": {
            "const": "orphaned"
          }
        },
        "required": [
          "state"
        ]
      },
      "then": {
        "properties": {
          "archive_phase": {
            "enum": [
              "orphaned"
            ]
          }
        },
        "required": [
          "archive_phase"
        ]
      }
    },
    {
      "if": {
        "properties": {
          "state": {
            "const": "discarding"
          }
        },
        "required": [
          "state"
        ]
      },
      "then": {
        "properties": {
          "archive_phase": {
            "enum": [
              "discarding"
            ]
          }
        },
        "required": [
          "archive_phase"
        ]
      }
    },
    {
      "if": {
        "properties": {
          "archive_phase": {
            "const": "retry_wait"
          }
        },
        "required": [
          "archive_phase"
        ]
      },
      "then": {
        "properties": {
          "archive_next_attempt_at": {
            "minLength": 1,
            "type": "string"
          },
          "latest_failure": {
            "minLength": 1,
            "type": "string"
          }
        }
      }
    },
    {
      "if": {
        "properties": {
          "archive_phase": {
            "const": "finalization_queued"
          }
        },
        "required": [
          "archive_phase"
        ]
      },
      "then": {
        "properties": {
          "archive_next_attempt_at": {
            "minLength": 1,
            "type": "string"
          },
          "latest_failure": {
            "type": "null"
          }
        }
      }
    },
    {
      "if": {
        "properties": {
          "archive_phase": {
            "not": {
              "enum": [
                "finalization_queued",
                "retry_wait"
              ]
            }
          }
        },
        "required": [
          "archive_phase"
        ]
      },
      "then": {
        "properties": {
          "archive_next_attempt_at": {
            "type": "null"
          }
        }
      }
    }
  ],
  "properties": {
    "archive_next_attempt_at": {
      "anyOf": [
        {
          "pattern": "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{6}Z$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Archive Next Attempt At"
    },
    "archive_phase": {
      "enum": [
        "planning",
        "uploading",
        "finalization_queued",
        "finalizing",
        "retry_wait",
        "completed",
        "canceled",
        "orphaned",
        "discarding"
      ],
      "title": "Archive Phase",
      "type": "string"
    },
    "archive_phase_updated_at": {
      "pattern": "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{6}Z$",
      "title": "Archive Phase Updated At",
      "type": "string"
    },
    "archive_root_sha256": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Archive Root Sha256"
    },
    "archive_storage_prefix": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Archive Storage Prefix"
    },
    "archive_store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "archive_total_bytes": {
      "anyOf": [
        {
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Archive Total Bytes"
    },
    "archive_total_units": {
      "anyOf": [
        {
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Archive Total Units"
    },
    "archive_uploaded_bytes": {
      "anyOf": [
        {
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Archive Uploaded Bytes"
    },
    "archive_uploaded_units": {
      "anyOf": [
        {
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Archive Uploaded Units"
    },
    "bytes_total": {
      "minimum": 0,
      "title": "Bytes Total",
      "type": "integer"
    },
    "collection": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CollectionSummaryOut"
        },
        {
          "type": "null"
        }
      ]
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "content_identity": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Content Identity"
    },
    "created_at": {
      "title": "Created At",
      "type": "string"
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
    "files_total": {
      "minimum": 0,
      "title": "Files Total",
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
    "latest_failure": {
      "anyOf": [
        {
          "maxLength": 1000,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Latest Failure"
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
    "provenance_identity": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Provenance Identity"
    },
    "provenance_mode": {
      "enum": [
        "captured",
        "mixed",
        "omitted"
      ],
      "title": "Provenance Mode",
      "type": "string"
    },
    "registration_constraints": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CollectionUploadRegistrationConstraintsOut"
        },
        {
          "type": "null"
        }
      ]
    },
    "resumed": {
      "title": "Resumed",
      "type": "boolean"
    },
    "state": {
      "enum": [
        "open",
        "closing",
        "uploading",
        "finalizing",
        "finalized",
        "canceled",
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
    "provenance_mode",
    "archive_store",
    "encryption_format",
    "passphrase_id",
    "state",
    "custody_mode",
    "registration_constraints",
    "files_total",
    "bytes_total",
    "upload_state_expires_at",
    "custody",
    "orphaned_at",
    "latest_failure",
    "archive_phase",
    "archive_phase_updated_at",
    "archive_next_attempt_at",
    "collection",
    "resumed"
  ],
  "title": "CreateOrResumeCollectionUploadSessionOut",
  "type": "object"
}
```

</details>
