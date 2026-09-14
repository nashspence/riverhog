# riverhog_protocol.CapturedFileProvenanceBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-capturedfileprovenancebinding:cd6e15239d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-039a76c3bb"></a>
- <a id="s-40e5f62b70"></a>`distribution`: `riverhog-protocol`
- <a id="s-dff7b28bfb"></a>`module`: `riverhog_protocol`
- <a id="s-a66d13507a"></a>`name`: `CapturedFileProvenanceBinding`
- <a id="s-9e88557392"></a>`unit`: `export`

### Declared structure

- <a id="s-d3f1323b10"></a>`kind`: `"class"`
- <a id="s-430f2670f8"></a>`signature`: `"\"(*, journal_id: ProvenanceJournalId, current_state_id: ProvenanceStateId, status: Literal['captured']) -> None\""`

#### Validated model schema

<a id="s-9d3f69681b"></a>
- <a id="s-e42d0b169f"></a>`title`: CapturedFileProvenanceBinding
- <a id="s-065b635651"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2ecae4a157"></a>`current_state_id` | yes | #/$defs/ProvenanceStateId |  |
| <a id="s-6f62b15f91"></a>`journal_id` | yes | #/$defs/ProvenanceJournalId |  |
| <a id="s-278b52194f"></a>`status` | yes | type="string"; const="captured" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-737542a142"></a>`ProvenanceJournalId` | type="string"; pattern="^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$" |
| <a id="s-ceb85e8f0a"></a>`ProvenanceStateId` | type="string"; pattern="^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$" |

## Governing policies

- <a id="pa-51a36f9abe"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CapturedFileProvenanceBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4b0b394124da2b47860fd563322116126382baab8f642046611991418957646e -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ProvenanceJournalId": {
          "pattern": "^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
          "type": "string"
        },
        "ProvenanceStateId": {
          "pattern": "^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
          "type": "string"
        }
      },
      "additionalProperties": false,
      "properties": {
        "current_state_id": {
          "$ref": "#/$defs/ProvenanceStateId"
        },
        "journal_id": {
          "$ref": "#/$defs/ProvenanceJournalId"
        },
        "status": {
          "const": "captured",
          "title": "Status",
          "type": "string"
        }
      },
      "required": [
        "journal_id",
        "current_state_id",
        "status"
      ],
      "title": "CapturedFileProvenanceBinding",
      "type": "object"
    },
    "signature": "\"(*, journal_id: ProvenanceJournalId, current_state_id: ProvenanceStateId, status: Literal['captured']) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CapturedFileProvenanceBinding",
  "unit": "export"
}
```
