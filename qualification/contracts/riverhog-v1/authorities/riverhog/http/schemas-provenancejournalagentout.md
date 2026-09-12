# schemas: ProvenanceJournalAgentOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-provenancejournalagentout:fa36aa6f7e -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProvenanceJournalAgentOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract summary

- `title`: ProvenanceJournalAgentOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `agent_id` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f583f1b6cd0a63844e10b628bea016e3aa237fb18e598f3e20a7dd739a3d0151 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "agent_id": {
      "title": "Agent Id",
      "type": "string"
    }
  },
  "required": [
    "agent_id"
  ],
  "title": "ProvenanceJournalAgentOut",
  "type": "object"
}
```
