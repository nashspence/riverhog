# schemas: ProvenanceTraceJournalItemOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-provenancetracejournalitemout:670a5f832c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: ProvenanceTraceJournalItemOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `journal` | yes | #/components/schemas/ProvenanceJournalOut |  |
| `kind` | yes | type="string"; const="journal" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ProvenanceJournalOut](schemas-provenancejournalout.md)

## Governing policies

- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProvenanceTraceJournalItemOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 60175e4be5e1b36c1cfca848086a59e5d4cca9f8a0223bc4e4d3c01d2fc1aaad -->

```json
{
  "additionalProperties": false,
  "properties": {
    "journal": {
      "$ref": "#/components/schemas/ProvenanceJournalOut"
    },
    "kind": {
      "const": "journal",
      "title": "Kind",
      "type": "string"
    }
  },
  "required": [
    "kind",
    "journal"
  ],
  "title": "ProvenanceTraceJournalItemOut",
  "type": "object"
}
```
