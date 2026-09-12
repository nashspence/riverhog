# schemas: CatalogSyncCheckpoint

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-catalogsynccheckpoint:a0ea4d55c4 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CatalogSyncCheckpoint`

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
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=4096, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: CatalogSyncCheckpoint
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `authorization_view_identity` | yes | string |  |
| `catalog_cursor` | yes | string |  |
| `format` | no | string |  |
| `source_identity` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 23f1a9cd803ee925c19334eab9c8b6024bbbee97eb6be8c601b45924f2bf8d9e -->

```json
{
  "additionalProperties": false,
  "properties": {
    "authorization_view_identity": {
      "maxLength": 64,
      "minLength": 64,
      "pattern": "^[0-9a-f]{64}$",
      "title": "Authorization View Identity",
      "type": "string"
    },
    "catalog_cursor": {
      "maxLength": 4096,
      "minLength": 1,
      "title": "Catalog Cursor",
      "type": "string"
    },
    "format": {
      "const": "riverhog-catalog-sync/v1",
      "default": "riverhog-catalog-sync/v1",
      "title": "Format",
      "type": "string"
    },
    "source_identity": {
      "maxLength": 64,
      "minLength": 64,
      "pattern": "^[0-9a-f]{64}$",
      "title": "Source Identity",
      "type": "string"
    }
  },
  "required": [
    "source_identity",
    "authorization_view_identity",
    "catalog_cursor"
  ],
  "title": "CatalogSyncCheckpoint",
  "type": "object"
}
```
