# riverhog-ftp-custody: claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-ftp-custody:riverhog-ftp-custody-claim:17bd35cae6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-custody](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-121468c8a2"></a>

- Document: `claim`

### Document schema

- `kind`: `"json-document"`
<a id="s-53f07a05ce"></a>

- <a id="s-bde77f1331"></a>`type`: `"object"`
- <a id="s-d0380f68f9"></a>`additionalProperties`: `false`
- <a id="s-eb62378bb5"></a>`required`: `["format","claim_id","source_event_id","source","files","journals"]`
- <a id="s-b0a3a8097d"></a>`title`: `"FtpClaimState"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-624e514537"></a>`claim_id` | yes | type="string"; minLength=1; title="Claim Id" |  |
| <a id="s-350d0b880f"></a>`completion_event_ids` | no | anyOf=[(type="array"; items=(type="string")); (type="null")]; default=null; title="Completion Event Ids" |  |
| <a id="s-5dd5626cf9"></a>`files` | yes | type="array"; items=([FtpClaimFileState](#s-16b4ff7d91)); minItems=1; title="Files" |  |
| <a id="s-59ff4956dc"></a>`format` | yes | type="string"; const="riverhog-ftp-adapter-claim/v1"; title="Format" |  |
| <a id="s-200634d957"></a>`journals` | yes | type="object"; additionalProperties=(type="string"); title="Journals" |  |
| <a id="s-5783ef5049"></a>`source` | yes | type="string"; minLength=1; title="Source" |  |
| <a id="s-1600aec5fa"></a>`source_event_id` | yes | type="string"; minLength=1; title="Source Event Id" |  |

#### Definitions

- [CapturedProvenanceState](#s-11aa5d2c36)
- [CompletionRecordState](#s-85d2e24360)
- [FtpClaimFileState](#s-16b4ff7d91)
- [OmittedProvenanceState](#s-9bcafae0b1)

#### <a id="s-11aa5d2c36"></a>definition `CapturedProvenanceState`

- <a id="s-e5a2084756"></a>`type`: `"object"`
- <a id="s-bb50f1381b"></a>`additionalProperties`: `false`
- <a id="s-827ab888ec"></a>`required`: `["path","bytes","sha256","status","journal_id","current_state_id"]`
- <a id="s-49c204eca6"></a>`title`: `"CapturedProvenanceState"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-59db85b53c"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-bf76fa2520"></a>`current_state_id` | yes | type="string"; minLength=1; title="Current State Id" |  |
| <a id="s-6ded817cb6"></a>`journal_id` | yes | type="string"; minLength=1; title="Journal Id" |  |
| <a id="s-bb7e9c745a"></a>`path` | yes | type="string"; minLength=1; title="Path" |  |
| <a id="s-c2f5a99cec"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |
| <a id="s-ea8481fd72"></a>`status` | yes | type="string"; const="captured"; title="Status" |  |

#### <a id="s-85d2e24360"></a>definition `CompletionRecordState`

- <a id="s-bb3a1f64e0"></a>`type`: `"object"`
- <a id="s-6e1be7d0a5"></a>`additionalProperties`: `false`
- <a id="s-401e2c16cf"></a>`required`: `["format","event_id","source_id","path","custody","bytes","device","inode"]`
- <a id="s-719245d5dd"></a>`title`: `"CompletionRecordState"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ccabd96792"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-42b21cdc9a"></a>`custody` | yes | type="string"; minLength=1; title="Custody" |  |
| <a id="s-5746c5531c"></a>`device` | yes | type="integer"; minimum=0; title="Device" |  |
| <a id="s-7f4867d982"></a>`event_id` | yes | type="string"; minLength=1; title="Event Id" |  |
| <a id="s-4a5d92e6d8"></a>`format` | yes | type="string"; const="riverhog-ftp-completion-record/v1"; title="Format" |  |
| <a id="s-735567f53f"></a>`inode` | yes | type="integer"; minimum=0; title="Inode" |  |
| <a id="s-cb825df170"></a>`path` | yes | type="string"; minLength=1; title="Path" |  |
| <a id="s-b7c3510f9d"></a>`source_id` | yes | type="string"; minLength=1; title="Source Id" |  |

#### <a id="s-16b4ff7d91"></a>definition `FtpClaimFileState`

- <a id="s-7da1dce1f0"></a>`type`: `"object"`
- <a id="s-9df2b8abe5"></a>`additionalProperties`: `false`
- <a id="s-611b4a187b"></a>`required`: `["path","bytes","sha256","device","inode","original","provenance"]`
- <a id="s-60cd2fd8da"></a>`title`: `"FtpClaimFileState"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-090727027e"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-8f3c32b43c"></a>`completion_record` | no | anyOf=[([CompletionRecordState](#s-85d2e24360)); (type="null")]; default=null |  |
| <a id="s-ad73184375"></a>`device` | yes | type="integer"; minimum=0; title="Device" |  |
| <a id="s-580e01dc1a"></a>`inode` | yes | type="integer"; minimum=0; title="Inode" |  |
| <a id="s-0f75c99e66"></a>`original` | yes | type="string"; minLength=1; title="Original" |  |
| <a id="s-14b8d91149"></a>`path` | yes | type="string"; minLength=1; title="Path" |  |
| <a id="s-bc12e2032e"></a>`provenance` | yes | anyOf=[([CapturedProvenanceState](#s-11aa5d2c36)); ([OmittedProvenanceState](#s-9bcafae0b1))]; title="Provenance" |  |
| <a id="s-3c672fd99a"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

#### <a id="s-9bcafae0b1"></a>definition `OmittedProvenanceState`

- <a id="s-2bc5d8cc41"></a>`type`: `"object"`
- <a id="s-66cfbe64e8"></a>`additionalProperties`: `false`
- <a id="s-cf8e522e5f"></a>`required`: `["path","bytes","sha256","status","omission_reason"]`
- <a id="s-a5e0cfb46f"></a>`title`: `"OmittedProvenanceState"`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-804a17ecd7"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-2df37f2887"></a>`omission_reason` | yes | type="string"; minLength=1; title="Omission Reason" |  |
| <a id="s-b914d1d581"></a>`path` | yes | type="string"; minLength=1; title="Path" |  |
| <a id="s-5d7f77aedf"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |
| <a id="s-e0466263dd"></a>`status` | yes | type="string"; const="omitted"; title="Status" |  |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-ftp-custody-durable-state-identity.md)

## Governing policies

- <a id="pa-f978cf0126"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-ftp-custody](../../../evidence/sources/authorities.md#src-54f88a3a47) — [reference/riverhog/ingress/ftp/src/riverhog\_ftp\_adapter/state\_contract.py::ftp\_custody\_state\_contract](../../../../../../reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/state_contract.py)

### Machine authority

- `/external_contract/durable_state/owners/6/structure/units/2`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
