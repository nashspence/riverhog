# schemas: CollectionUploadSessionOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionuploadsessionout:4e6413f60e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-8ffd85b110"></a>

- <a id="s-fd3c253146"></a>`type`: `"object"`
- <a id="s-1f0a6f08f5"></a>`additionalProperties`: `false`
- <a id="s-03440553db"></a>`required`: `["collection_id","created_at","ingest_source","description","description_revision","description_identity","description_publication","tag_publication","tag_count","provenance_mode","archive_store","encryption_format","passphrase_id","state","custody_mode","registration_constraints","files_total","bytes_total","upload_state_expires_at","custody","orphaned_at","latest_failure","archive_phase","archive_phase_updated_at","archive_next_attempt_at","collection"]`
- <a id="s-0fcc1ecfde"></a>`title`: `"CollectionUploadSessionOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9b1773688a"></a>`archive_next_attempt_at` | yes | anyOf=[(type="string"; pattern="^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{6}Z$"); (type="null")]; title="Archive Next Attempt At" |  |
| <a id="s-b6f24c5453"></a>`archive_phase` | yes | type="string"; enum=["planning","uploading","finalization_queued","finalizing","retry_wait","completed","canceled","orphaned","discarding"]; title="Archive Phase" |  |
| <a id="s-40cb395743"></a>`archive_phase_updated_at` | yes | type="string"; pattern="^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{6}Z$"; title="Archive Phase Updated At" |  |
| <a id="s-4c2695d822"></a>`archive_root_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Archive Root Sha256" |  |
| <a id="s-c6e3f15926"></a>`archive_storage_prefix` | no | anyOf=[(type="string"); (type="null")]; title="Archive Storage Prefix" |  |
| <a id="s-b0b2fc7a0d"></a>`archive_store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |
| <a id="s-e70dac8ff9"></a>`archive_total_bytes` | no | anyOf=[(type="integer"); (type="null")]; title="Archive Total Bytes" |  |
| <a id="s-08a6059292"></a>`archive_total_units` | no | anyOf=[(type="integer"); (type="null")]; title="Archive Total Units" |  |
| <a id="s-5f8835c2ce"></a>`archive_uploaded_bytes` | no | anyOf=[(type="integer"); (type="null")]; title="Archive Uploaded Bytes" |  |
| <a id="s-e173f08920"></a>`archive_uploaded_units` | no | anyOf=[(type="integer"); (type="null")]; title="Archive Uploaded Units" |  |
| <a id="s-11002853d8"></a>`bytes_total` | yes | type="integer"; minimum=0; title="Bytes Total" |  |
| <a id="s-69ad9fc366"></a>`collection` | yes | anyOf=[([CollectionSummaryOut](schemas-collectionsummaryout.md)); (type="null")] |  |
| <a id="s-fdc801eb0a"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-b9651ea42e"></a>`content_identity` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Content Identity" |  |
| <a id="s-bece9bbffb"></a>`created_at` | yes | type="string"; title="Created At" |  |
| <a id="s-c267172c78"></a>`custody` | yes | discriminator={"mapping":{"complete":"#/components/schemas/CompleteCollectionUploadCustodyOut","pending":"#/components/schemas/PendingCollectionUploadCustodyOut"},"propertyName":"state"}; oneOf=[([PendingCollectionUploadCustodyOut](schemas-pendingcollectionuploadcustodyout.md)); ([CompleteCollectionUploadCustodyOut](schemas-completecollectionuploadcustodyout.md))]; title="Custody" |  |
| <a id="s-449911e290"></a>`custody_mode` | yes | type="string"; enum=["producer-retained","custody-transfer"]; title="Custody Mode" |  |
| <a id="s-91c9b60eef"></a>`description` | yes | anyOf=[([CollectionDescription](schemas-collectiondescription.md)); (type="null")] |  |
| <a id="s-0e3caca7dc"></a>`description_identity` | yes | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Description Identity" |  |
| <a id="s-406d74e184"></a>`description_publication` | yes | type="string"; enum=["pending","not_required","current"]; title="Description Publication" |  |
| <a id="s-17a2e2a4fd"></a>`description_revision` | yes | anyOf=[(type="integer"; minimum=0; maximum=9007199254740991); (type="null")]; title="Description Revision" |  |
| <a id="s-e561d8b174"></a>`encryption_format` | yes | type="string"; title="Encryption Format" |  |
| <a id="s-10f5a3b3b8"></a>`files_total` | yes | type="integer"; minimum=0; title="Files Total" |  |
| <a id="s-54879215e4"></a>`ingest_source` | yes | anyOf=[(type="string"); (type="null")]; title="Ingest Source" |  |
| <a id="s-2c574f18dd"></a>`latest_failure` | yes | anyOf=[(type="string"; maxLength=1000; minLength=1); (type="null")]; title="Latest Failure" |  |
| <a id="s-43ab285f83"></a>`orphaned_at` | yes | anyOf=[(type="string"); (type="null")]; title="Orphaned At" |  |
| <a id="s-6dac1eb9bc"></a>`passphrase_id` | yes | type="string"; pattern="^[A-Za-z0-9_-]{16,128}$"; title="Passphrase Id" |  |
| <a id="s-13b5f1d3eb"></a>`provenance_identity` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Provenance Identity" |  |
| <a id="s-8447ff2de7"></a>`provenance_mode` | yes | type="string"; enum=["captured","mixed","omitted"]; title="Provenance Mode" |  |
| <a id="s-ae9f4ae61b"></a>`registration_constraints` | yes | anyOf=[([CollectionUploadRegistrationConstraintsOut](schemas-collectionuploadregistrationconstraintsout.md)); (type="null")] |  |
| <a id="s-ecc4798871"></a>`state` | yes | type="string"; enum=["open","closing","uploading","finalizing","finalized","canceled","orphaned","discarding"]; title="State" |  |
| <a id="s-0513821e14"></a>`tag_count` | yes | type="integer"; minimum=0; title="Tag Count" |  |
| <a id="s-ad623850e5"></a>`tag_publication` | yes | type="string"; enum=["pending","current"]; title="Tag Publication" |  |
| <a id="s-9b310b8936"></a>`tag_revision` | no | anyOf=[(type="integer"; minimum=1; maximum=9007199254740991); (type="null")]; title="Tag Revision" |  |
| <a id="s-5e83624c1e"></a>`tag_set_identity` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Tag Set Identity" |  |
| <a id="s-ddf168beca"></a>`upload_state_expires_at` | yes | anyOf=[(type="string"); (type="null")]; title="Upload State Expires At" |  |

### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-ece32ec12b"></a>1 | properties={state: (const="finalized")} | properties={archive_root_sha256: (type="string"); collection: (type="object"); content_identity: (type="string"); registration_constraints: (type="null"); tag_publication: (const="current"); tag_revision: (type="integer"); tag_set_identity: (type="string")} | properties={archive_root_sha256: (type="null"); collection: (type="null"); content_identity: (type="null"); registration_constraints: (type="object")} |
| <a id="s-052c2dfd86"></a>2 | properties={tag_publication: (const="current")} | properties={tag_revision: (type="integer"); tag_set_identity: (type="string")} | no additional constraint |
| <a id="s-ea42661cb0"></a>3 | properties={state: (const="finalized")}; required=["state"] | oneOf=[(properties={provenance_identity: (type="string"; pattern="^[0-9a-f]{64}$"); provenance_mode: (enum=["captured","mixed"])}; required=["provenance_mode","provenance_identity"]); (properties={provenance_identity: (type="null"); provenance_mode: (const="omitted")}; required=["provenance_mode","provenance_identity"])] | properties={provenance_identity: (type="null"); provenance_mode: (enum=["captured","omitted"])} |
| <a id="s-b94c19da9b"></a>4 | properties={custody_mode: (const="producer-retained")}; required=["custody_mode"] | properties={orphaned_at: (type="null"); state: (enum=["open","uploading","finalizing","canceled","finalized"]); upload_state_expires_at: (type="null")} | no additional constraint |
| <a id="s-6a9935e8f3"></a>5 | properties={state: (enum=["orphaned","discarding"])}; required=["state"] | properties={custody_mode: (const="custody-transfer"); orphaned_at: (type="string"); upload_state_expires_at: (type="null")} | no additional constraint |
| <a id="s-ac067f3585"></a>6 | properties={custody_mode: (const="custody-transfer"); state: (enum=["open","closing"])}; required=["custody_mode","state"] | properties={orphaned_at: (type="null"); upload_state_expires_at: (type="string")} | no additional constraint |
| <a id="s-ef257400b3"></a>7 | properties={custody_mode: (const="custody-transfer"); state: (enum=["uploading","finalizing","canceled","finalized"])}; required=["custody_mode","state"] | properties={orphaned_at: (type="null"); upload_state_expires_at: (type="null")} | no additional constraint |
| <a id="s-dbea157926"></a>8 | properties={state: (enum=["finalizing","finalized"])}; required=["state"] | properties={custody: (properties={state: (const="complete")}; required=["state"])} | no additional constraint |
| <a id="s-bd5c9462ee"></a>9 | properties={custody_mode: (const="custody-transfer"); state: (const="uploading")}; required=["custody_mode","state"] | properties={custody: (properties={state: (const="complete")}; required=["state"])} | no additional constraint |
| <a id="s-032ec9a428"></a>10 | properties={state: (const="open")}; required=["state"] | properties={archive_phase: (enum=["planning","uploading"])}; required=["archive_phase"] | no additional constraint |
| <a id="s-0b3f209dbe"></a>11 | properties={state: (const="closing")}; required=["state"] | properties={archive_phase: (enum=["uploading"])}; required=["archive_phase"] | no additional constraint |
| <a id="s-ff3c380b6d"></a>12 | properties={state: (const="uploading")}; required=["state"] | properties={archive_phase: (enum=["uploading"])}; required=["archive_phase"] | no additional constraint |
| <a id="s-ae159b4ba5"></a>13 | properties={state: (const="finalizing")}; required=["state"] | properties={archive_phase: (enum=["finalization_queued","finalizing","retry_wait"])}; required=["archive_phase"] | no additional constraint |
| <a id="s-7a52d7b20b"></a>14 | properties={state: (const="finalized")}; required=["state"] | properties={archive_phase: (enum=["completed"])}; required=["archive_phase"] | no additional constraint |
| <a id="s-ca4c7972dc"></a>15 | properties={state: (const="canceled")}; required=["state"] | properties={archive_phase: (enum=["canceled"])}; required=["archive_phase"] | no additional constraint |
| <a id="s-c16ef629a8"></a>16 | properties={state: (const="orphaned")}; required=["state"] | properties={archive_phase: (enum=["orphaned"])}; required=["archive_phase"] | no additional constraint |
| <a id="s-bece3f9199"></a>17 | properties={state: (const="discarding")}; required=["state"] | properties={archive_phase: (enum=["discarding"])}; required=["archive_phase"] | no additional constraint |
| <a id="s-9e02429bff"></a>18 | properties={archive_phase: (const="retry_wait")}; required=["archive_phase"] | properties={archive_next_attempt_at: (type="string"; minLength=1); latest_failure: (type="string"; minLength=1)} | no additional constraint |
| <a id="s-6afd82a485"></a>19 | properties={archive_phase: (const="finalization_queued")}; required=["archive_phase"] | properties={archive_next_attempt_at: (type="string"; minLength=1); latest_failure: (type="null")} | no additional constraint |
| <a id="s-cce856fcd8"></a>20 | properties={archive_phase: (not=(enum=["finalization_queued","retry_wait"]))}; required=["archive_phase"] | properties={archive_next_attempt_at: (type="null")} | no additional constraint |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-848466ef26"></a>[allOf alternative 3 · then · oneOf alternative 1 · field provenance_identity](#s-ea42661cb0) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-03d76d63b1"></a>[field archive_root_sha256 · string value](#s-4c2695d822) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-733b493f9e"></a>[field content_identity · string value](#s-b9651ea42e) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-e62e1e660f"></a>[field description_identity · string value](#s-0e3caca7dc) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-73865bea2d"></a>[field description_revision · integer value](#s-17a2e2a4fd) | `value · schema-value · contract_max` | maximum=9007199254740991; minimum=0; reason="schema-maximum" |
| <a id="s-81b6124850"></a>[field latest_failure · string value](#s-2c574f18dd) | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |
| <a id="s-b5a5c73241"></a>[field provenance_identity · string value](#s-13b5f1d3eb) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-a5ad0a343d"></a>[field tag_revision · integer value](#s-9b310b8936) | `value · schema-value · contract_max` | maximum=9007199254740991; minimum=1; reason="schema-maximum" |
| <a id="s-4a8d210801"></a>[field tag_set_identity · string value](#s-5e83624c1e) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract dossiers

- [ArchiveStoreName](schemas-archivestorename.md)
- [CollectionDescription](schemas-collectiondescription.md)
- [CollectionId](schemas-collectionid.md)
- [CollectionSummaryOut](schemas-collectionsummaryout.md)
- [CollectionUploadRegistrationConstraintsOut](schemas-collectionuploadregistrationconstraintsout.md)
- [CompleteCollectionUploadCustodyOut](schemas-completecollectionuploadcustodyout.md)
- [PendingCollectionUploadCustodyOut](schemas-pendingcollectionuploadcustodyout.md)

## Governing policies

- <a id="pa-f79e7cfaab"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-e27e41d393"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadSessionOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 816c881bc844b2d22cf0e1a3a6f125d974607d6261e5ba44495e14e033eb36ac -->

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
    "collection"
  ],
  "title": "CollectionUploadSessionOut",
  "type": "object"
}
```

</details>
