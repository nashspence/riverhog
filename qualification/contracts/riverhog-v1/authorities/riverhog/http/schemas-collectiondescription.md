# schemas: CollectionDescription

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectiondescription:12d7c3bc08 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionDescription`

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
| encoded-size | bytes | `contract_max` | maximum=32768, reason=bounded-human-authored-catalog-description |
| length | characters | `contract_max` | maximum=32768, minimum=1, reason=schema-maximum |

## Contract summary

- `type`: string

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: daefae928b8ba365eb083c697fe3d6d1635c9772e1689e8785f4b8c33183cf9a -->

```json
{
  "maxLength": 32768,
  "minLength": 1,
  "type": "string",
  "x-riverhog-encoded-bytes-max": 32768,
  "x-riverhog-extent": {
    "policy": "contract_max",
    "reason": "bounded-human-authored-catalog-description"
  },
  "x-unicode-normalization": "NFC"
}
```
