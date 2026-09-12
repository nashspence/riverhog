# schemas: ProvenanceTraceItemOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-provenancetraceitemout:a9f4a84490 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProvenanceTraceItemOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ProvenanceTraceExternalStateReferenceItemOut](schemas-provenancetraceexternalstatereferenceitemout.md)
- [schemas: ProvenanceTraceJournalItemOut](schemas-provenancetracejournalitemout.md)

## Contract summary

| Field | Shape |
|---|---|
| `discriminator` | object (2 fields) |
| `oneOf` | array (2 items) |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 87420641aa260811cc4582e164bbd05cee21465ad23b0e05d84c4feef7205ddc -->

```json
{
  "discriminator": {
    "mapping": {
      "external_state_reference": "#/components/schemas/ProvenanceTraceExternalStateReferenceItemOut",
      "journal": "#/components/schemas/ProvenanceTraceJournalItemOut"
    },
    "propertyName": "kind"
  },
  "oneOf": [
    {
      "$ref": "#/components/schemas/ProvenanceTraceJournalItemOut"
    },
    {
      "$ref": "#/components/schemas/ProvenanceTraceExternalStateReferenceItemOut"
    }
  ]
}
```
