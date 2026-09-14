# riverhog_protocol.RiverhogEventPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-riverhogeventpage:b756e6c28e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-34f582a2ca"></a>
- <a id="s-70c2b349dc"></a>`distribution`: `riverhog-protocol`
- <a id="s-b261a446ec"></a>`module`: `riverhog_protocol`
- <a id="s-f0b2a3acab"></a>`name`: `RiverhogEventPage`
- <a id="s-79f392f105"></a>`unit`: `export`

### Declared structure

- <a id="s-7185cbf2f1"></a>`kind`: `"class"`
- <a id="s-2220813a11"></a>`signature`: `"'(*, events: list[RiverhogLifecycleEvent], next_cursor: LifecycleEventCursor, has_more: bool) -> None'"`

#### Validated model schema

<a id="s-1e0a8aa4fc"></a>
- <a id="s-f3fdecf5b7"></a>`title`: RiverhogEventPage
- <a id="s-424e2b5528"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-75f3ba19a8"></a>`events` | yes | type="array"; items=(#/$defs/RiverhogLifecycleEvent) |  |
| <a id="s-3e40fb765d"></a>`has_more` | yes | type="boolean" |  |
| <a id="s-b862758468"></a>`next_cursor` | yes | #/$defs/LifecycleEventCursor |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-cfa4c816fe"></a>`ArchiveCopyCanceledData` | type="object"; fields=`actor`, `cause`, `collection_created_at`, `collection_id`, `context`, `destination_store`, `initiator`, `source_store`, `state`; additional keys=`additionalProperties`, `required` |
| <a id="s-2d7985a904"></a>`ArchiveCopyCanceledEvent` | type="object"; fields=`data`, `datacontenttype`, `id`, `source`, `specversion`, `subject`, `time`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-4ea8ff49ee"></a>`ArchiveCopyCompletedData` | type="object"; fields=`actor`, `cause`, `collection_created_at`, `collection_id`, `context`, `destination_store`, `initiator`, `source_store`, `state`; additional keys=`additionalProperties`, `required` |
| <a id="s-0f544f1f69"></a>`ArchiveCopyCompletedEvent` | type="object"; fields=`data`, `datacontenttype`, `id`, `source`, `specversion`, `subject`, `time`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-a5bfba4563"></a>`ArchiveCopyIssueData` | type="object"; fields=`actor`, `cause`, `collection_created_at`, `collection_id`, `context`, `destination_store`, `error`, `initiator`, `source_store`, `state`; additional keys=`additionalProperties`, `required` |
| <a id="s-c13ccdbeaf"></a>`ArchiveCopyIssueEvent` | type="object"; fields=`data`, `datacontenttype`, `id`, `source`, `specversion`, `subject`, `time`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-336f4457d3"></a>`ArchiveCopyRequestedData` | type="object"; fields=`actor`, `cause`, `collection_created_at`, `collection_id`, `context`, `destination_store`, `initiator`, `source_store`, `state`; additional keys=`additionalProperties`, `required` |
| <a id="s-7f20e5b5e8"></a>`ArchiveCopyRequestedEvent` | type="object"; fields=`data`, `datacontenttype`, `id`, `source`, `specversion`, `subject`, `time`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-2e2a7afe9a"></a>`ArchiveStoreName` | type="string"; pattern="^[a-z0-9]+(?:-[a-z0-9]+)*$" |
| <a id="s-04e497d16f"></a>`CollectionDeletedData` | type="object"; fields=`actor`, `bytes`, `cause`, `collection_created_at`, `collection_id`, `context`, `files`, `initiator`, `remote_storage_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-e18a1ebf85"></a>`CollectionDeletedEvent` | type="object"; fields=`data`, `datacontenttype`, `id`, `source`, `specversion`, `subject`, `time`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-caf8e4a999"></a>`CollectionFinalizedData` | type="object"; fields=`actor`, `archive_root_sha256`, `bytes_total`, `cause`, `collection_created_at`, `collection_id`, `context`, `files_total`, `initiator`; additional keys=`additionalProperties`, `required` |
| <a id="s-dcb5f23831"></a>`CollectionFinalizedEvent` | type="object"; fields=`data`, `datacontenttype`, `id`, `source`, `specversion`, `subject`, `time`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-3b7d7e3c2c"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-99b2d14f05"></a>`LifecycleEventCursor` | type="string"; minLength=1; maxLength=19; pattern="^(?:0\|[1-9][0-9]*)$" |
| <a id="s-8f780dd62d"></a>`RetrievalCanceledData` | type="object"; fields=`actor`, `cause`, `collection_created_at`, `collection_id`, `collection_ids`, `context`, `initiator`, `reason`, `retrieval_id`, `state`; additional keys=`additionalProperties`, `required` |
| <a id="s-e80951e313"></a>`RetrievalCanceledEvent` | type="object"; fields=`data`, `datacontenttype`, `id`, `source`, `specversion`, `subject`, `time`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-e680525602"></a>`RetrievalCompletedData` | type="object"; fields=`actor`, `cause`, `collection_created_at`, `collection_id`, `collection_ids`, `context`, `initiator`, `retrieval_id`, `state`; additional keys=`additionalProperties`, `required` |
| <a id="s-e763e9511f"></a>`RetrievalCompletedEvent` | type="object"; fields=`data`, `datacontenttype`, `id`, `source`, `specversion`, `subject`, `time`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-901491f41d"></a>`RetrievalExpiredData` | type="object"; fields=`actor`, `cause`, `collection_created_at`, `collection_id`, `collection_ids`, `context`, `initiator`, `retrieval_id`, `state`; additional keys=`additionalProperties`, `required` |
| <a id="s-cd3863d78c"></a>`RetrievalExpiredEvent` | type="object"; fields=`data`, `datacontenttype`, `id`, `source`, `specversion`, `subject`, `time`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-c2a74431c4"></a>`RetrievalFailedData` | type="object"; fields=`actor`, `cause`, `collection_created_at`, `collection_id`, `collection_ids`, `context`, `error`, `initiator`, `retrieval_id`, `state`; additional keys=`additionalProperties`, `required` |
| <a id="s-f7876916d6"></a>`RetrievalFailedEvent` | type="object"; fields=`data`, `datacontenttype`, `id`, `source`, `specversion`, `subject`, `time`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-5d019111c7"></a>`RetrievalIssueData` | type="object"; fields=`actor`, `cause`, `collection_created_at`, `collection_id`, `collection_ids`, `context`, `error`, `initiator`, `retrieval_id`, `state`; additional keys=`additionalProperties`, `required` |
| <a id="s-061dcaf609"></a>`RetrievalIssueEvent` | type="object"; fields=`data`, `datacontenttype`, `id`, `source`, `specversion`, `subject`, `time`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-94276b17d8"></a>`RetrievalReadyData` | type="object"; fields=`actor`, `cause`, `collection_created_at`, `collection_id`, `collection_ids`, `context`, `expires_at`, `initiator`, `retrieval_id`, `state`; additional keys=`additionalProperties`, `required` |
| <a id="s-f23d561333"></a>`RetrievalReadyEvent` | type="object"; fields=`data`, `datacontenttype`, `id`, `source`, `specversion`, `subject`, `time`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-d1bc1f3107"></a>`RetrievalRenewedData` | type="object"; fields=`actor`, `cause`, `collection_created_at`, `collection_id`, `collection_ids`, `context`, `expires_at`, `initiator`, `retrieval_id`, `state`; additional keys=`additionalProperties`, `required` |
| <a id="s-45fb1a016c"></a>`RetrievalRenewedEvent` | type="object"; fields=`data`, `datacontenttype`, `id`, `source`, `specversion`, `subject`, `time`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-64049bf6f9"></a>`RetrievalRequestedData` | type="object"; fields=`actor`, `cause`, `collection_created_at`, `collection_id`, `collection_ids`, `context`, `files`, `initiator`, `objects`, `restore_required`, `retrieval_id`, `state`; additional keys=`additionalProperties`, `required` |
| <a id="s-9dad1abe42"></a>`RetrievalRequestedEvent` | type="object"; fields=`data`, `datacontenttype`, `id`, `source`, `specversion`, `subject`, `time`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-3ec07c9031"></a>`RiverhogActor` | type="object"; fields=`app`, `key_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-56c61376dd"></a>`RiverhogEventCause` | type="object"; fields=`id`, `source`, `subject`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-41ab5ca15a"></a>`RiverhogLifecycleEvent` | oneOf=#/$defs/CollectionFinalizedEvent \| #/$defs/CollectionDeletedEvent \| #/$defs/ArchiveCopyRequestedEvent \| #/$defs/ArchiveCopyCompletedEvent \| #/$defs/ArchiveCopyIssueEvent \| #/$defs/ArchiveCopyCanceledEvent \| #/$defs/RetrievalRequestedEvent \| #/$defs/RetrievalReadyEvent \| #/$defs/RetrievalRenewedEvent \| #/$defs/RetrievalCompletedEvent \| #/$defs/RetrievalCanceledEvent \| #/$defs/RetrievalExpiredEvent \| #/$defs/RetrievalIssueEvent \| #/$defs/RetrievalFailedEvent; additional keys=`discriminator` |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.RiverhogEventPage.require_progress_after](riverhog-protocol-riverhogeventpage-require-progress-after.md)

## Governing policies

- <a id="pa-962d15f316"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.RiverhogEventPage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b87219ec551e73ffbb568e8a324ed98ddc89e5b193e414fbef4a551b54fa9e30 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArchiveCopyCanceledData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "maxLength": 64,
              "minLength": 1,
              "title": "Collection Created At",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Context"
            },
            "destination_store": {
              "$ref": "#/$defs/ArchiveStoreName"
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "source_store": {
              "$ref": "#/$defs/ArchiveStoreName"
            },
            "state": {
              "const": "canceled",
              "title": "State",
              "type": "string"
            }
          },
          "required": [
            "actor",
            "initiator",
            "collection_id",
            "collection_created_at",
            "source_store",
            "destination_store",
            "state"
          ],
          "title": "ArchiveCopyCanceledData",
          "type": "object"
        },
        "ArchiveCopyCanceledEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/ArchiveCopyCanceledData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "title": "Datacontenttype",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "title": "Id",
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "title": "Source",
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "title": "Specversion",
              "type": "string"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Subject"
            },
            "time": {
              "title": "Time",
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.archive_copy.canceled",
              "title": "Type",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
          ],
          "title": "ArchiveCopyCanceledEvent",
          "type": "object"
        },
        "ArchiveCopyCompletedData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "maxLength": 64,
              "minLength": 1,
              "title": "Collection Created At",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Context"
            },
            "destination_store": {
              "$ref": "#/$defs/ArchiveStoreName"
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "source_store": {
              "$ref": "#/$defs/ArchiveStoreName"
            },
            "state": {
              "const": "completed",
              "title": "State",
              "type": "string"
            }
          },
          "required": [
            "actor",
            "initiator",
            "collection_id",
            "collection_created_at",
            "source_store",
            "destination_store",
            "state"
          ],
          "title": "ArchiveCopyCompletedData",
          "type": "object"
        },
        "ArchiveCopyCompletedEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/ArchiveCopyCompletedData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "title": "Datacontenttype",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "title": "Id",
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "title": "Source",
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "title": "Specversion",
              "type": "string"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Subject"
            },
            "time": {
              "title": "Time",
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.archive_copy.completed",
              "title": "Type",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
          ],
          "title": "ArchiveCopyCompletedEvent",
          "type": "object"
        },
        "ArchiveCopyIssueData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "maxLength": 64,
              "minLength": 1,
              "title": "Collection Created At",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Context"
            },
            "destination_store": {
              "$ref": "#/$defs/ArchiveStoreName"
            },
            "error": {
              "maxLength": 16384,
              "minLength": 1,
              "title": "Error",
              "type": "string"
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "source_store": {
              "$ref": "#/$defs/ArchiveStoreName"
            },
            "state": {
              "const": "failed",
              "title": "State",
              "type": "string"
            }
          },
          "required": [
            "actor",
            "initiator",
            "collection_id",
            "collection_created_at",
            "source_store",
            "destination_store",
            "state",
            "error"
          ],
          "title": "ArchiveCopyIssueData",
          "type": "object"
        },
        "ArchiveCopyIssueEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/ArchiveCopyIssueData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "title": "Datacontenttype",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "title": "Id",
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "title": "Source",
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "title": "Specversion",
              "type": "string"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Subject"
            },
            "time": {
              "title": "Time",
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.archive_copy.issue",
              "title": "Type",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
          ],
          "title": "ArchiveCopyIssueEvent",
          "type": "object"
        },
        "ArchiveCopyRequestedData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "maxLength": 64,
              "minLength": 1,
              "title": "Collection Created At",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Context"
            },
            "destination_store": {
              "$ref": "#/$defs/ArchiveStoreName"
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "source_store": {
              "$ref": "#/$defs/ArchiveStoreName"
            },
            "state": {
              "const": "requested",
              "title": "State",
              "type": "string"
            }
          },
          "required": [
            "actor",
            "initiator",
            "collection_id",
            "collection_created_at",
            "source_store",
            "destination_store",
            "state"
          ],
          "title": "ArchiveCopyRequestedData",
          "type": "object"
        },
        "ArchiveCopyRequestedEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/ArchiveCopyRequestedData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "title": "Datacontenttype",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "title": "Id",
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "title": "Source",
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "title": "Specversion",
              "type": "string"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Subject"
            },
            "time": {
              "title": "Time",
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.archive_copy.requested",
              "title": "Type",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
          ],
          "title": "ArchiveCopyRequestedEvent",
          "type": "object"
        },
        "ArchiveStoreName": {
          "pattern": "^[a-z0-9]+(?:-[a-z0-9]+)*$",
          "type": "string"
        },
        "CollectionDeletedData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "bytes": {
              "minimum": 0,
              "title": "Bytes",
              "type": "integer"
            },
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "maxLength": 64,
              "minLength": 1,
              "title": "Collection Created At",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Context"
            },
            "files": {
              "minimum": 0,
              "title": "Files",
              "type": "integer"
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "remote_storage_bytes": {
              "minimum": 0,
              "title": "Remote Storage Bytes",
              "type": "integer"
            }
          },
          "required": [
            "actor",
            "initiator",
            "collection_id",
            "collection_created_at",
            "files",
            "bytes",
            "remote_storage_bytes"
          ],
          "title": "CollectionDeletedData",
          "type": "object"
        },
        "CollectionDeletedEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/CollectionDeletedData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "title": "Datacontenttype",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "title": "Id",
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "title": "Source",
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "title": "Specversion",
              "type": "string"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Subject"
            },
            "time": {
              "title": "Time",
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.collection.deleted",
              "title": "Type",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
          ],
          "title": "CollectionDeletedEvent",
          "type": "object"
        },
        "CollectionFinalizedData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "archive_root_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Archive Root Sha256",
              "type": "string"
            },
            "bytes_total": {
              "minimum": 0,
              "title": "Bytes Total",
              "type": "integer"
            },
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "maxLength": 64,
              "minLength": 1,
              "title": "Collection Created At",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Context"
            },
            "files_total": {
              "minimum": 0,
              "title": "Files Total",
              "type": "integer"
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            }
          },
          "required": [
            "actor",
            "initiator",
            "collection_id",
            "collection_created_at",
            "files_total",
            "bytes_total",
            "archive_root_sha256"
          ],
          "title": "CollectionFinalizedData",
          "type": "object"
        },
        "CollectionFinalizedEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/CollectionFinalizedData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "title": "Datacontenttype",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "title": "Id",
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "title": "Source",
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "title": "Specversion",
              "type": "string"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Subject"
            },
            "time": {
              "title": "Time",
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.collection.finalized",
              "title": "Type",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
          ],
          "title": "CollectionFinalizedEvent",
          "type": "object"
        },
        "CollectionId": {
          "minimum": 1,
          "type": "integer"
        },
        "LifecycleEventCursor": {
          "maxLength": 19,
          "minLength": 1,
          "pattern": "^(?:0|[1-9][0-9]*)$",
          "type": "string"
        },
        "RetrievalCanceledData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "anyOf": [
                {
                  "maxLength": 64,
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Collection Created At"
            },
            "collection_id": {
              "anyOf": [
                {
                  "$ref": "#/$defs/CollectionId"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_ids": {
              "items": {
                "$ref": "#/$defs/CollectionId"
              },
              "minItems": 1,
              "title": "Collection Ids",
              "type": "array"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Context"
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "reason": {
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
              "default": null,
              "title": "Reason"
            },
            "retrieval_id": {
              "maxLength": 300,
              "minLength": 1,
              "title": "Retrieval Id",
              "type": "string"
            },
            "state": {
              "const": "canceled",
              "title": "State",
              "type": "string"
            }
          },
          "required": [
            "actor",
            "initiator",
            "retrieval_id",
            "collection_ids",
            "state"
          ],
          "title": "RetrievalCanceledData",
          "type": "object"
        },
        "RetrievalCanceledEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/RetrievalCanceledData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "title": "Datacontenttype",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "title": "Id",
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "title": "Source",
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "title": "Specversion",
              "type": "string"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Subject"
            },
            "time": {
              "title": "Time",
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.retrieval.canceled",
              "title": "Type",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
          ],
          "title": "RetrievalCanceledEvent",
          "type": "object"
        },
        "RetrievalCompletedData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "anyOf": [
                {
                  "maxLength": 64,
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Collection Created At"
            },
            "collection_id": {
              "anyOf": [
                {
                  "$ref": "#/$defs/CollectionId"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_ids": {
              "items": {
                "$ref": "#/$defs/CollectionId"
              },
              "minItems": 1,
              "title": "Collection Ids",
              "type": "array"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Context"
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "retrieval_id": {
              "maxLength": 300,
              "minLength": 1,
              "title": "Retrieval Id",
              "type": "string"
            },
            "state": {
              "const": "completed",
              "title": "State",
              "type": "string"
            }
          },
          "required": [
            "actor",
            "initiator",
            "retrieval_id",
            "collection_ids",
            "state"
          ],
          "title": "RetrievalCompletedData",
          "type": "object"
        },
        "RetrievalCompletedEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/RetrievalCompletedData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "title": "Datacontenttype",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "title": "Id",
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "title": "Source",
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "title": "Specversion",
              "type": "string"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Subject"
            },
            "time": {
              "title": "Time",
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.retrieval.completed",
              "title": "Type",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
          ],
          "title": "RetrievalCompletedEvent",
          "type": "object"
        },
        "RetrievalExpiredData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "anyOf": [
                {
                  "maxLength": 64,
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Collection Created At"
            },
            "collection_id": {
              "anyOf": [
                {
                  "$ref": "#/$defs/CollectionId"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_ids": {
              "items": {
                "$ref": "#/$defs/CollectionId"
              },
              "minItems": 1,
              "title": "Collection Ids",
              "type": "array"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Context"
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "retrieval_id": {
              "maxLength": 300,
              "minLength": 1,
              "title": "Retrieval Id",
              "type": "string"
            },
            "state": {
              "const": "expired",
              "title": "State",
              "type": "string"
            }
          },
          "required": [
            "actor",
            "initiator",
            "retrieval_id",
            "collection_ids",
            "state"
          ],
          "title": "RetrievalExpiredData",
          "type": "object"
        },
        "RetrievalExpiredEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/RetrievalExpiredData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "title": "Datacontenttype",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "title": "Id",
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "title": "Source",
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "title": "Specversion",
              "type": "string"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Subject"
            },
            "time": {
              "title": "Time",
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.retrieval.expired",
              "title": "Type",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
          ],
          "title": "RetrievalExpiredEvent",
          "type": "object"
        },
        "RetrievalFailedData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "anyOf": [
                {
                  "maxLength": 64,
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Collection Created At"
            },
            "collection_id": {
              "anyOf": [
                {
                  "$ref": "#/$defs/CollectionId"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_ids": {
              "items": {
                "$ref": "#/$defs/CollectionId"
              },
              "minItems": 1,
              "title": "Collection Ids",
              "type": "array"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Context"
            },
            "error": {
              "maxLength": 16384,
              "minLength": 1,
              "title": "Error",
              "type": "string"
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "retrieval_id": {
              "maxLength": 300,
              "minLength": 1,
              "title": "Retrieval Id",
              "type": "string"
            },
            "state": {
              "const": "failed",
              "title": "State",
              "type": "string"
            }
          },
          "required": [
            "actor",
            "initiator",
            "retrieval_id",
            "collection_ids",
            "state",
            "error"
          ],
          "title": "RetrievalFailedData",
          "type": "object"
        },
        "RetrievalFailedEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/RetrievalFailedData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "title": "Datacontenttype",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "title": "Id",
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "title": "Source",
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "title": "Specversion",
              "type": "string"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Subject"
            },
            "time": {
              "title": "Time",
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.retrieval.failed",
              "title": "Type",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
          ],
          "title": "RetrievalFailedEvent",
          "type": "object"
        },
        "RetrievalIssueData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "anyOf": [
                {
                  "maxLength": 64,
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Collection Created At"
            },
            "collection_id": {
              "anyOf": [
                {
                  "$ref": "#/$defs/CollectionId"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_ids": {
              "items": {
                "$ref": "#/$defs/CollectionId"
              },
              "minItems": 1,
              "title": "Collection Ids",
              "type": "array"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Context"
            },
            "error": {
              "maxLength": 16384,
              "minLength": 1,
              "title": "Error",
              "type": "string"
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "retrieval_id": {
              "maxLength": 300,
              "minLength": 1,
              "title": "Retrieval Id",
              "type": "string"
            },
            "state": {
              "const": "requested",
              "title": "State",
              "type": "string"
            }
          },
          "required": [
            "actor",
            "initiator",
            "retrieval_id",
            "collection_ids",
            "state",
            "error"
          ],
          "title": "RetrievalIssueData",
          "type": "object"
        },
        "RetrievalIssueEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/RetrievalIssueData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "title": "Datacontenttype",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "title": "Id",
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "title": "Source",
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "title": "Specversion",
              "type": "string"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Subject"
            },
            "time": {
              "title": "Time",
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.retrieval.issue",
              "title": "Type",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
          ],
          "title": "RetrievalIssueEvent",
          "type": "object"
        },
        "RetrievalReadyData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "anyOf": [
                {
                  "maxLength": 64,
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Collection Created At"
            },
            "collection_id": {
              "anyOf": [
                {
                  "$ref": "#/$defs/CollectionId"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_ids": {
              "items": {
                "$ref": "#/$defs/CollectionId"
              },
              "minItems": 1,
              "title": "Collection Ids",
              "type": "array"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Context"
            },
            "expires_at": {
              "maxLength": 64,
              "minLength": 1,
              "title": "Expires At",
              "type": "string"
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "retrieval_id": {
              "maxLength": 300,
              "minLength": 1,
              "title": "Retrieval Id",
              "type": "string"
            },
            "state": {
              "const": "ready",
              "title": "State",
              "type": "string"
            }
          },
          "required": [
            "actor",
            "initiator",
            "retrieval_id",
            "collection_ids",
            "state",
            "expires_at"
          ],
          "title": "RetrievalReadyData",
          "type": "object"
        },
        "RetrievalReadyEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/RetrievalReadyData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "title": "Datacontenttype",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "title": "Id",
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "title": "Source",
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "title": "Specversion",
              "type": "string"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Subject"
            },
            "time": {
              "title": "Time",
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.retrieval.ready",
              "title": "Type",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
          ],
          "title": "RetrievalReadyEvent",
          "type": "object"
        },
        "RetrievalRenewedData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "anyOf": [
                {
                  "maxLength": 64,
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Collection Created At"
            },
            "collection_id": {
              "anyOf": [
                {
                  "$ref": "#/$defs/CollectionId"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_ids": {
              "items": {
                "$ref": "#/$defs/CollectionId"
              },
              "minItems": 1,
              "title": "Collection Ids",
              "type": "array"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Context"
            },
            "expires_at": {
              "maxLength": 64,
              "minLength": 1,
              "title": "Expires At",
              "type": "string"
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "retrieval_id": {
              "maxLength": 300,
              "minLength": 1,
              "title": "Retrieval Id",
              "type": "string"
            },
            "state": {
              "const": "ready",
              "title": "State",
              "type": "string"
            }
          },
          "required": [
            "actor",
            "initiator",
            "retrieval_id",
            "collection_ids",
            "state",
            "expires_at"
          ],
          "title": "RetrievalRenewedData",
          "type": "object"
        },
        "RetrievalRenewedEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/RetrievalRenewedData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "title": "Datacontenttype",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "title": "Id",
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "title": "Source",
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "title": "Specversion",
              "type": "string"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Subject"
            },
            "time": {
              "title": "Time",
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.retrieval.renewed",
              "title": "Type",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
          ],
          "title": "RetrievalRenewedEvent",
          "type": "object"
        },
        "RetrievalRequestedData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "anyOf": [
                {
                  "maxLength": 64,
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Collection Created At"
            },
            "collection_id": {
              "anyOf": [
                {
                  "$ref": "#/$defs/CollectionId"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_ids": {
              "items": {
                "$ref": "#/$defs/CollectionId"
              },
              "minItems": 1,
              "title": "Collection Ids",
              "type": "array"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Context"
            },
            "files": {
              "minimum": 1,
              "title": "Files",
              "type": "integer"
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "objects": {
              "minimum": 1,
              "title": "Objects",
              "type": "integer"
            },
            "restore_required": {
              "title": "Restore Required",
              "type": "boolean"
            },
            "retrieval_id": {
              "maxLength": 300,
              "minLength": 1,
              "title": "Retrieval Id",
              "type": "string"
            },
            "state": {
              "enum": [
                "requested",
                "ready"
              ],
              "title": "State",
              "type": "string"
            }
          },
          "required": [
            "actor",
            "initiator",
            "retrieval_id",
            "collection_ids",
            "state",
            "files",
            "objects",
            "restore_required"
          ],
          "title": "RetrievalRequestedData",
          "type": "object"
        },
        "RetrievalRequestedEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/RetrievalRequestedData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "title": "Datacontenttype",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "title": "Id",
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "title": "Source",
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "title": "Specversion",
              "type": "string"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Subject"
            },
            "time": {
              "title": "Time",
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.retrieval.requested",
              "title": "Type",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
          ],
          "title": "RetrievalRequestedEvent",
          "type": "object"
        },
        "RiverhogActor": {
          "additionalProperties": false,
          "properties": {
            "app": {
              "maxLength": 160,
              "minLength": 1,
              "title": "App",
              "type": "string"
            },
            "key_id": {
              "anyOf": [
                {
                  "maxLength": 300,
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Key Id"
            }
          },
          "required": [
            "app"
          ],
          "title": "RiverhogActor",
          "type": "object"
        },
        "RiverhogEventCause": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "maxLength": 300,
              "minLength": 1,
              "title": "Id",
              "type": "string"
            },
            "source": {
              "maxLength": 1000,
              "minLength": 1,
              "title": "Source",
              "type": "string"
            },
            "subject": {
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
              "default": null,
              "title": "Subject"
            },
            "type": {
              "maxLength": 300,
              "minLength": 1,
              "title": "Type",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type"
          ],
          "title": "RiverhogEventCause",
          "type": "object"
        },
        "RiverhogLifecycleEvent": {
          "discriminator": {
            "mapping": {
              "io.riverhog.riverhog.archive_copy.canceled": "#/$defs/ArchiveCopyCanceledEvent",
              "io.riverhog.riverhog.archive_copy.completed": "#/$defs/ArchiveCopyCompletedEvent",
              "io.riverhog.riverhog.archive_copy.issue": "#/$defs/ArchiveCopyIssueEvent",
              "io.riverhog.riverhog.archive_copy.requested": "#/$defs/ArchiveCopyRequestedEvent",
              "io.riverhog.riverhog.collection.deleted": "#/$defs/CollectionDeletedEvent",
              "io.riverhog.riverhog.collection.finalized": "#/$defs/CollectionFinalizedEvent",
              "io.riverhog.riverhog.retrieval.canceled": "#/$defs/RetrievalCanceledEvent",
              "io.riverhog.riverhog.retrieval.completed": "#/$defs/RetrievalCompletedEvent",
              "io.riverhog.riverhog.retrieval.expired": "#/$defs/RetrievalExpiredEvent",
              "io.riverhog.riverhog.retrieval.failed": "#/$defs/RetrievalFailedEvent",
              "io.riverhog.riverhog.retrieval.issue": "#/$defs/RetrievalIssueEvent",
              "io.riverhog.riverhog.retrieval.ready": "#/$defs/RetrievalReadyEvent",
              "io.riverhog.riverhog.retrieval.renewed": "#/$defs/RetrievalRenewedEvent",
              "io.riverhog.riverhog.retrieval.requested": "#/$defs/RetrievalRequestedEvent"
            },
            "propertyName": "type"
          },
          "oneOf": [
            {
              "$ref": "#/$defs/CollectionFinalizedEvent"
            },
            {
              "$ref": "#/$defs/CollectionDeletedEvent"
            },
            {
              "$ref": "#/$defs/ArchiveCopyRequestedEvent"
            },
            {
              "$ref": "#/$defs/ArchiveCopyCompletedEvent"
            },
            {
              "$ref": "#/$defs/ArchiveCopyIssueEvent"
            },
            {
              "$ref": "#/$defs/ArchiveCopyCanceledEvent"
            },
            {
              "$ref": "#/$defs/RetrievalRequestedEvent"
            },
            {
              "$ref": "#/$defs/RetrievalReadyEvent"
            },
            {
              "$ref": "#/$defs/RetrievalRenewedEvent"
            },
            {
              "$ref": "#/$defs/RetrievalCompletedEvent"
            },
            {
              "$ref": "#/$defs/RetrievalCanceledEvent"
            },
            {
              "$ref": "#/$defs/RetrievalExpiredEvent"
            },
            {
              "$ref": "#/$defs/RetrievalIssueEvent"
            },
            {
              "$ref": "#/$defs/RetrievalFailedEvent"
            }
          ]
        }
      },
      "additionalProperties": false,
      "properties": {
        "events": {
          "items": {
            "$ref": "#/$defs/RiverhogLifecycleEvent"
          },
          "title": "Events",
          "type": "array"
        },
        "has_more": {
          "title": "Has More",
          "type": "boolean"
        },
        "next_cursor": {
          "$ref": "#/$defs/LifecycleEventCursor"
        }
      },
      "required": [
        "events",
        "next_cursor",
        "has_more"
      ],
      "title": "RiverhogEventPage",
      "type": "object"
    },
    "signature": "'(*, events: list[RiverhogLifecycleEvent], next_cursor: LifecycleEventCursor, has_more: bool) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "RiverhogEventPage",
  "unit": "export"
}
```
