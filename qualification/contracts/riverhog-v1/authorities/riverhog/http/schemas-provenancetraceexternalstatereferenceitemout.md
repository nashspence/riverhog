# schemas: ProvenanceTraceExternalStateReferenceItemOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-provenancetraceexternalstatereferenceitemout:a173c9863b -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProvenanceTraceExternalStateReferenceItemOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ProvenanceExternalStateReferenceOut](schemas-provenanceexternalstatereferenceout.md)

## Contract summary

- `title`: ProvenanceTraceExternalStateReferenceItemOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `kind` | yes | string |  |
| `reference` | yes | #/components/schemas/ProvenanceExternalStateReferenceOut |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 14775eb0d4995fb0d8d72c90bed4aef8f5075c75e67f45ac5a8720eaf6eb1e67 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "kind": {
      "const": "external_state_reference",
      "title": "Kind",
      "type": "string"
    },
    "reference": {
      "$ref": "#/components/schemas/ProvenanceExternalStateReferenceOut"
    }
  },
  "required": [
    "kind",
    "reference"
  ],
  "title": "ProvenanceTraceExternalStateReferenceItemOut",
  "type": "object"
}
```
