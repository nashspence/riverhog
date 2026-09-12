# schemas: BrowseQuery

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-browsequery:62820dc7fc -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/BrowseQuery`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=4096, minimum=1, reason=schema-maximum |

## Contract summary

- `type`: string

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 602c72fd196e5a6dcf80f03cb263d0c08cd81dc7587ae34af8973f95d8cfe9c6 -->

```json
{
  "maxLength": 4096,
  "minLength": 1,
  "pattern": "^\\S(?:[\\s\\S]*\\S)?$",
  "type": "string"
}
```
