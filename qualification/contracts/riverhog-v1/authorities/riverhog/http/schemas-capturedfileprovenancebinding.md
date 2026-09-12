# schemas: CapturedFileProvenanceBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-capturedfileprovenancebinding:61c9a53c26 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CapturedFileProvenanceBinding`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ProvenanceJournalId](schemas-provenancejournalid.md)
- [schemas: ProvenanceStateId](schemas-provenancestateid.md)

## Contract summary

- `title`: CapturedFileProvenanceBinding
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `current_state_id` | yes | #/components/schemas/ProvenanceStateId |  |
| `journal_id` | yes | #/components/schemas/ProvenanceJournalId |  |
| `status` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cf3ce6fcaf00f930e4e5a3c9173e96f05af02be3acf636affeb667fd35e2e11b -->

```json
{
  "additionalProperties": false,
  "properties": {
    "current_state_id": {
      "$ref": "#/components/schemas/ProvenanceStateId"
    },
    "journal_id": {
      "$ref": "#/components/schemas/ProvenanceJournalId"
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
}
```
