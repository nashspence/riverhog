# schemas: ProvenanceExternalStateReferenceOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-provenanceexternalstatereferenceout:27323ab20c -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProvenanceExternalStateReferenceOut`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ProvenanceEntryId](schemas-provenanceentryid.md)
- [schemas: ProvenanceJournalId](schemas-provenancejournalid.md)
- [schemas: ProvenanceStateId](schemas-provenancestateid.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: ProvenanceExternalStateReferenceOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `entry_id` | yes | #/components/schemas/ProvenanceEntryId |  |
| `entry_json_sha256` | yes | string |  |
| `from_journal_id` | yes | #/components/schemas/ProvenanceJournalId |  |
| `state_id` | yes | #/components/schemas/ProvenanceStateId |  |
| `to_journal_id` | yes | #/components/schemas/ProvenanceJournalId |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0be22da19b0a8f539bd3883f9540fdeb932c4ee27c46a109f44a17090f8e9865 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "entry_id": {
      "$ref": "#/components/schemas/ProvenanceEntryId"
    },
    "entry_json_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Entry Json Sha256",
      "type": "string"
    },
    "from_journal_id": {
      "$ref": "#/components/schemas/ProvenanceJournalId"
    },
    "state_id": {
      "$ref": "#/components/schemas/ProvenanceStateId"
    },
    "to_journal_id": {
      "$ref": "#/components/schemas/ProvenanceJournalId"
    }
  },
  "required": [
    "from_journal_id",
    "to_journal_id",
    "state_id",
    "entry_id",
    "entry_json_sha256"
  ],
  "title": "ProvenanceExternalStateReferenceOut",
  "type": "object"
}
```
