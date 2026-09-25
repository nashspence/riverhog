# a-riverhog-ftp-spool-custody: claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:a-riverhog-ftp-spool-custody:a-riverhog-ftp-spool-custody-claim:3e33ee8319 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool-custody](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-aa2ffc6ec0"></a>

- Document: `claim`

### Document schema

- `kind`: `"json-document"`
<a id="s-a228ffa869"></a>

- <a id="s-e4d610ace1"></a>`type`: `"object"`
- <a id="s-b5c8e32cff"></a>`additionalProperties`: `false`
- <a id="s-086d336c32"></a>`required`: `["format","claim_id","source_event_id","source","files","journals"]`
- <a id="s-a43f29c41a"></a>`title`: `"FtpClaimState"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f5f04b925d"></a>`claim_id` | yes | type="string"; minLength=1; title="Claim Id" |  |
| <a id="s-6b20a4b70e"></a>`completion_event_ids` | no | anyOf=[(type="array"; items=(type="string")); (type="null")]; default=null; title="Completion Event Ids" |  |
| <a id="s-c9ba3737ad"></a>`files` | yes | type="array"; items=([FtpClaimFileState](#s-625fdcbed1)); minItems=1; title="Files" |  |
| <a id="s-98291d0730"></a>`format` | yes | type="string"; const="a-riverhog-ftp-spool-claim/v1"; title="Format" |  |
| <a id="s-c8b539c399"></a>`journals` | yes | type="object"; additionalProperties=(type="string"); title="Journals" |  |
| <a id="s-79724d6665"></a>`source` | yes | type="string"; minLength=1; title="Source" |  |
| <a id="s-f22ba3246c"></a>`source_event_id` | yes | type="string"; minLength=1; title="Source Event Id" |  |

#### Definitions

- [CapturedProvenanceState](#s-25f4e011aa)
- [CompletionRecordState](#s-be329af1ed)
- [FtpClaimFileState](#s-625fdcbed1)
- [OmittedProvenanceState](#s-a7eb521a49)

#### <a id="s-25f4e011aa"></a>definition `CapturedProvenanceState`

- <a id="s-6d05237f69"></a>`type`: `"object"`
- <a id="s-667953d3d2"></a>`additionalProperties`: `false`
- <a id="s-8503547738"></a>`required`: `["path","bytes","sha256","status","journal_id","current_state_id"]`
- <a id="s-b4dd5e7de4"></a>`title`: `"CapturedProvenanceState"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-523596ef08"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-baf9c6260e"></a>`current_state_id` | yes | type="string"; minLength=1; title="Current State Id" |  |
| <a id="s-634f9d7116"></a>`journal_id` | yes | type="string"; minLength=1; title="Journal Id" |  |
| <a id="s-ea8f0e60d0"></a>`path` | yes | type="string"; minLength=1; title="Path" |  |
| <a id="s-fd97b1b37c"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |
| <a id="s-3e64c648a6"></a>`status` | yes | type="string"; const="captured"; title="Status" |  |

#### <a id="s-be329af1ed"></a>definition `CompletionRecordState`

- <a id="s-de0d251fd9"></a>`type`: `"object"`
- <a id="s-f6203eb97f"></a>`additionalProperties`: `false`
- <a id="s-8d47a65750"></a>`required`: `["format","event_id","source_id","path","custody","bytes","device","inode"]`
- <a id="s-ef5a524302"></a>`title`: `"CompletionRecordState"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-214761c17b"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-f2c30c8004"></a>`custody` | yes | type="string"; minLength=1; title="Custody" |  |
| <a id="s-751e8f510b"></a>`device` | yes | type="integer"; minimum=0; title="Device" |  |
| <a id="s-aea2fd23f9"></a>`event_id` | yes | type="string"; minLength=1; title="Event Id" |  |
| <a id="s-8112e43458"></a>`format` | yes | type="string"; const="riverhog-ftp-completion-record/v1"; title="Format" |  |
| <a id="s-7942c344b4"></a>`inode` | yes | type="integer"; minimum=0; title="Inode" |  |
| <a id="s-e2fef4d2ef"></a>`path` | yes | type="string"; minLength=1; title="Path" |  |
| <a id="s-65d54f76b4"></a>`source_id` | yes | type="string"; minLength=1; title="Source Id" |  |

#### <a id="s-625fdcbed1"></a>definition `FtpClaimFileState`

- <a id="s-290445e0f6"></a>`type`: `"object"`
- <a id="s-cc3dba7116"></a>`additionalProperties`: `false`
- <a id="s-0d4a367f14"></a>`required`: `["path","bytes","sha256","device","inode","original","provenance"]`
- <a id="s-63c2dcd02c"></a>`title`: `"FtpClaimFileState"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f6219ae377"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-12e6c63650"></a>`completion_record` | no | anyOf=[([CompletionRecordState](#s-be329af1ed)); (type="null")]; default=null |  |
| <a id="s-41f24d585b"></a>`device` | yes | type="integer"; minimum=0; title="Device" |  |
| <a id="s-bba168b7af"></a>`inode` | yes | type="integer"; minimum=0; title="Inode" |  |
| <a id="s-b17a568b27"></a>`original` | yes | type="string"; minLength=1; title="Original" |  |
| <a id="s-8690e68158"></a>`path` | yes | type="string"; minLength=1; title="Path" |  |
| <a id="s-76d722bf35"></a>`provenance` | yes | anyOf=[([CapturedProvenanceState](#s-25f4e011aa)); ([OmittedProvenanceState](#s-a7eb521a49))]; title="Provenance" |  |
| <a id="s-c403e4df49"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

#### <a id="s-a7eb521a49"></a>definition `OmittedProvenanceState`

- <a id="s-a7e7b95990"></a>`type`: `"object"`
- <a id="s-ff137cf2d1"></a>`additionalProperties`: `false`
- <a id="s-1836376ba3"></a>`required`: `["path","bytes","sha256","status","omission_reason"]`
- <a id="s-3323ebcd38"></a>`title`: `"OmittedProvenanceState"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-128141dbea"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-e68ff4c43e"></a>`omission_reason` | yes | type="string"; minLength=1; title="Omission Reason" |  |
| <a id="s-b3f59a33ac"></a>`path` | yes | type="string"; minLength=1; title="Path" |  |
| <a id="s-8ff4a12e9b"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |
| <a id="s-f3c93409c5"></a>`status` | yes | type="string"; const="omitted"; title="Status" |  |

## Maintained corroboration

### Related interface records

- [Schema identity](a-riverhog-ftp-spool-custody-durable-state-identity.md)

## Governing policies

- <a id="pa-be132a6d21"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:a-riverhog-ftp-spool-custody](../../../evidence/sources/authorities.md#src-97232522a5) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/state\_contract.py::ftp\_custody\_state\_contract](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/state_contract.py)

### Machine authority

- `/external_contract/durable_state/owners/8/structure/units/2`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 402f7ea73ce0238423790a182bf354577f4ea8637de397793a0274f8c430ab0c -->

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
        "const": "a-riverhog-ftp-spool-claim/v1",
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

</details>
