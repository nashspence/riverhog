# schemas: ProvenanceTraceItemOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-provenancetraceitemout:a9f4a84490 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `discriminator` | additional keys=`mapping`, `propertyName` |
| `oneOf` | items=#/components/schemas/ProvenanceTraceJournalItemOut \| #/components/schemas/ProvenanceTraceExternalStateReferenceItemOut |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ProvenanceTraceExternalStateReferenceItemOut](schemas-provenancetraceexternalstatereferenceitemout.md)
- [schemas: ProvenanceTraceJournalItemOut](schemas-provenancetracejournalitemout.md)

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

- `/external_contract/http_openapi/riverhog/components/schemas/ProvenanceTraceItemOut`

### Exact owned JSON

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
