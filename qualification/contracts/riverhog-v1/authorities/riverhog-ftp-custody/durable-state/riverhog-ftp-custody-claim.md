# riverhog-ftp-custody: claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-ftp-custody:riverhog-ftp-custody-claim:17bd35cae6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-custody](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-121468c8a2"></a>
- Document: `claim`

### Document schema

<a id="s-53f07a05ce"></a>
- <a id="s-b0a3a8097d"></a>`title`: FtpClaimState
- <a id="s-bde77f1331"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-624e514537"></a>`claim_id` | yes | type="string"; minLength=1 |  |
| <a id="s-350d0b880f"></a>`completion_event_ids` | no | anyOf=type="array"; items=(type="string") \| type="null" |  |
| <a id="s-5dd5626cf9"></a>`files` | yes | type="array"; minItems=1; items=(#/$defs/FtpClaimFileState) |  |
| <a id="s-59ff4956dc"></a>`format` | yes | type="string"; const="riverhog-ftp-adapter-claim/v1" |  |
| <a id="s-200634d957"></a>`journals` | yes | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-5783ef5049"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-1600aec5fa"></a>`source_event_id` | yes | type="string"; minLength=1 |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-11aa5d2c36"></a>`CapturedProvenanceState` | type="object"; fields=`bytes`, `current_state_id`, `journal_id`, `path`, `sha256`, `status`; additional keys=`additionalProperties`, `required` |
| <a id="s-85d2e24360"></a>`CompletionRecordState` | type="object"; fields=`bytes`, `custody`, `device`, `event_id`, `format`, `inode`, `path`, `source_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-16b4ff7d91"></a>`FtpClaimFileState` | type="object"; fields=`bytes`, `completion_record`, `device`, `inode`, `original`, `path`, `provenance`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-9bcafae0b1"></a>`OmittedProvenanceState` | type="object"; fields=`bytes`, `omission_reason`, `path`, `sha256`, `status`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [riverhog-ftp-custody durable-state identity](riverhog-ftp-custody-durable-state-identity.md)

## Governing policies

- <a id="pa-f978cf0126"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-ftp-custody](../../../evidence/sources.md#src-54f88a3a47) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/state_contract.py`

### Machine authority

- `/external_contract/durable_state/owners/6/structure/units/2`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 05023043dc3819ecc3643620f9467bbf9c8624fd14e80bf07fecaf32a6ae2822 -->

```json
{
  "id": "claim",
  "kind": "json-document",
  "schema": {
    "$defs": {
      "CapturedProvenanceState": {
        "additionalProperties": false,
        "properties": {
          "bytes": {
            "minimum": 0,
            "title": "Bytes",
            "type": "integer"
          },
          "current_state_id": {
            "minLength": 1,
            "title": "Current State Id",
            "type": "string"
          },
          "journal_id": {
            "minLength": 1,
            "title": "Journal Id",
            "type": "string"
          },
          "path": {
            "minLength": 1,
            "title": "Path",
            "type": "string"
          },
          "sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Sha256",
            "type": "string"
          },
          "status": {
            "const": "captured",
            "title": "Status",
            "type": "string"
          }
        },
        "required": [
          "path",
          "bytes",
          "sha256",
          "status",
          "journal_id",
          "current_state_id"
        ],
        "title": "CapturedProvenanceState",
        "type": "object"
      },
      "CompletionRecordState": {
        "additionalProperties": false,
        "properties": {
          "bytes": {
            "minimum": 0,
            "title": "Bytes",
            "type": "integer"
          },
          "custody": {
            "minLength": 1,
            "title": "Custody",
            "type": "string"
          },
          "device": {
            "minimum": 0,
            "title": "Device",
            "type": "integer"
          },
          "event_id": {
            "minLength": 1,
            "title": "Event Id",
            "type": "string"
          },
          "format": {
            "const": "riverhog-ftp-completion-record/v1",
            "title": "Format",
            "type": "string"
          },
          "inode": {
            "minimum": 0,
            "title": "Inode",
            "type": "integer"
          },
          "path": {
            "minLength": 1,
            "title": "Path",
            "type": "string"
          },
          "source_id": {
            "minLength": 1,
            "title": "Source Id",
            "type": "string"
          }
        },
        "required": [
          "format",
          "event_id",
          "source_id",
          "path",
          "custody",
          "bytes",
          "device",
          "inode"
        ],
        "title": "CompletionRecordState",
        "type": "object"
      },
      "FtpClaimFileState": {
        "additionalProperties": false,
        "properties": {
          "bytes": {
            "minimum": 0,
            "title": "Bytes",
            "type": "integer"
          },
          "completion_record": {
            "anyOf": [
              {
                "$ref": "#/$defs/CompletionRecordState"
              },
              {
                "type": "null"
              }
            ],
            "default": null
          },
          "device": {
            "minimum": 0,
            "title": "Device",
            "type": "integer"
          },
          "inode": {
            "minimum": 0,
            "title": "Inode",
            "type": "integer"
          },
          "original": {
            "minLength": 1,
            "title": "Original",
            "type": "string"
          },
          "path": {
            "minLength": 1,
            "title": "Path",
            "type": "string"
          },
          "provenance": {
            "anyOf": [
              {
                "$ref": "#/$defs/CapturedProvenanceState"
              },
              {
                "$ref": "#/$defs/OmittedProvenanceState"
              }
            ],
            "title": "Provenance"
          },
          "sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Sha256",
            "type": "string"
          }
        },
        "required": [
          "path",
          "bytes",
          "sha256",
          "device",
          "inode",
          "original",
          "provenance"
        ],
        "title": "FtpClaimFileState",
        "type": "object"
      },
      "OmittedProvenanceState": {
        "additionalProperties": false,
        "properties": {
          "bytes": {
            "minimum": 0,
            "title": "Bytes",
            "type": "integer"
          },
          "omission_reason": {
            "minLength": 1,
            "title": "Omission Reason",
            "type": "string"
          },
          "path": {
            "minLength": 1,
            "title": "Path",
            "type": "string"
          },
          "sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Sha256",
            "type": "string"
          },
          "status": {
            "const": "omitted",
            "title": "Status",
            "type": "string"
          }
        },
        "required": [
          "path",
          "bytes",
          "sha256",
          "status",
          "omission_reason"
        ],
        "title": "OmittedProvenanceState",
        "type": "object"
      }
    },
    "additionalProperties": false,
    "properties": {
      "claim_id": {
        "minLength": 1,
        "title": "Claim Id",
        "type": "string"
      },
      "completion_event_ids": {
        "anyOf": [
          {
            "items": {
              "type": "string"
            },
            "type": "array"
          },
          {
            "type": "null"
          }
        ],
        "default": null,
        "title": "Completion Event Ids"
      },
      "files": {
        "items": {
          "$ref": "#/$defs/FtpClaimFileState"
        },
        "minItems": 1,
        "title": "Files",
        "type": "array"
      },
      "format": {
        "const": "riverhog-ftp-adapter-claim/v1",
        "title": "Format",
        "type": "string"
      },
      "journals": {
        "additionalProperties": {
          "type": "string"
        },
        "title": "Journals",
        "type": "object"
      },
      "source": {
        "minLength": 1,
        "title": "Source",
        "type": "string"
      },
      "source_event_id": {
        "minLength": 1,
        "title": "Source Event Id",
        "type": "string"
      }
    },
    "required": [
      "format",
      "claim_id",
      "source_event_id",
      "source",
      "files",
      "journals"
    ],
    "title": "FtpClaimState",
    "type": "object"
  }
}
```
