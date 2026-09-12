# schemas: ObservationEvidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-observationevidence:7cfc081818 -->

Complete routing evidence: immutable request plus accepted result.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: ObservationEvidence
- `description`: Complete routing evidence: immutable request plus accepted result.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `request` | yes | #/components/schemas/ObservationRequest |  |
| `result` | yes | #/components/schemas/ObservationResult |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ObservationRequest](schemas-observationrequest.md)
- [schemas: ObservationResult](schemas-observationresult.md)

## Governing policies

- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ObservationEvidence`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a06d5d9e675fcc32ff369ef0d620be72d9a5f4c330e1025f97210e12985be6cc -->

```json
{
  "additionalProperties": false,
  "description": "Complete routing evidence: immutable request plus accepted result.",
  "properties": {
    "request": {
      "$ref": "#/components/schemas/ObservationRequest"
    },
    "result": {
      "$ref": "#/components/schemas/ObservationResult"
    }
  },
  "required": [
    "request",
    "result"
  ],
  "title": "ObservationEvidence",
  "type": "object"
}
```
