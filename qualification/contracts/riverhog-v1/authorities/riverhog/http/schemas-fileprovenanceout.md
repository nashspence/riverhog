# schemas: _FileProvenanceOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-fileprovenanceout:75b44e6184 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/_FileProvenanceOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CapturedCollectionFileProvenanceOut](schemas-capturedcollectionfileprovenanceout.md)
- [schemas: OmittedCollectionFileProvenanceOut](schemas-omittedcollectionfileprovenanceout.md)

## Contract summary

| Field | Shape |
|---|---|
| `anyOf` | array (2 items) |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c1bc71fe1eca29d749d7950ecb0a31e22bd1a68d42d7a613e0193edaa0006f17 -->

```json
{
  "anyOf": [
    {
      "$ref": "#/components/schemas/CapturedCollectionFileProvenanceOut"
    },
    {
      "$ref": "#/components/schemas/OmittedCollectionFileProvenanceOut"
    }
  ]
}
```
