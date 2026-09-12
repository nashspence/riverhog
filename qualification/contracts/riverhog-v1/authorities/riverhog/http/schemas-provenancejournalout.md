# schemas: ProvenanceJournalOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-provenancejournalout:557e77cc2b -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProvenanceJournalOut`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CanonicalRelPath](schemas-canonicalrelpath.md)
- [schemas: ProvenanceJournalId](schemas-provenancejournalid.md)
- [schemas: ProvenanceStateId](schemas-provenancestateid.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: ProvenanceJournalOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `agent_count` | yes | integer |  |
| `bytes` | yes | integer |  |
| `current_bytes` | yes | integer |  |
| `current_path` | yes | #/components/schemas/CanonicalRelPath |  |
| `current_sha256` | yes | string |  |
| `current_state_id` | yes | #/components/schemas/ProvenanceStateId |  |
| `entity_counts` | yes | object |  |
| `entries` | yes | integer |  |
| `journal_id` | yes | #/components/schemas/ProvenanceJournalId |  |
| `sha256` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0b4d5bd09f7cf8ce6972fbf494eb28c6511c569fa3f79630616a0eb4b11f622f -->

```json
{
  "additionalProperties": false,
  "properties": {
    "agent_count": {
      "minimum": 0,
      "title": "Agent Count",
      "type": "integer"
    },
    "bytes": {
      "minimum": 0,
      "title": "Bytes",
      "type": "integer"
    },
    "current_bytes": {
      "minimum": 0,
      "title": "Current Bytes",
      "type": "integer"
    },
    "current_path": {
      "$ref": "#/components/schemas/CanonicalRelPath"
    },
    "current_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Current Sha256",
      "type": "string"
    },
    "current_state_id": {
      "$ref": "#/components/schemas/ProvenanceStateId"
    },
    "entity_counts": {
      "additionalProperties": {
        "type": "integer"
      },
      "title": "Entity Counts",
      "type": "object"
    },
    "entries": {
      "minimum": 1,
      "title": "Entries",
      "type": "integer"
    },
    "journal_id": {
      "$ref": "#/components/schemas/ProvenanceJournalId"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    }
  },
  "required": [
    "journal_id",
    "bytes",
    "sha256",
    "entries",
    "current_state_id",
    "current_path",
    "current_bytes",
    "current_sha256",
    "agent_count",
    "entity_counts"
  ],
  "title": "ProvenanceJournalOut",
  "type": "object"
}
```
