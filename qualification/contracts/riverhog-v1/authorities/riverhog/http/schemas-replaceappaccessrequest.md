# schemas: ReplaceAppAccessRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-replaceappaccessrequest:55bfbf2277 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ReplaceAppAccessRequest`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ApplicationAccessGrantSet](schemas-applicationaccessgrantset.md)

## Contract summary

- `title`: ReplaceAppAccessRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `access` | yes | #/components/schemas/ApplicationAccessGrantSet |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 95fe088d01d861de5cd6e4f12e8a679f80043db02021062e83bd52673b2cf245 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "access": {
      "$ref": "#/components/schemas/ApplicationAccessGrantSet"
    }
  },
  "required": [
    "access"
  ],
  "title": "ReplaceAppAccessRequest",
  "type": "object"
}
```
