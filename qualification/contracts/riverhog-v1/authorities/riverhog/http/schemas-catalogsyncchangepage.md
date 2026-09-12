# schemas: CatalogSyncChangePage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-catalogsyncchangepage:a10a7f5278 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## External contract

- `title`: CatalogSyncChangePage
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `authorization_view_identity` | yes | type="string"; minLength=64; maxLength=64; pattern="^[0-9a-f]{64}$" |  |
| `caught_up` | yes | type="boolean" |  |
| `changes` | yes | type="array"; maxItems=100; items=(oneOf=#/components/schemas/CatalogSyncUpsert \| #/components/schemas/CatalogSyncDelete; additional keys=`discriminator`) |  |
| `format` | no | type="string"; const="riverhog-catalog-sync/v1" |  |
| `next_cursor` | yes | type="string"; minLength=1; maxLength=4096 |  |
| `source_identity` | yes | type="string"; minLength=64; maxLength=64; pattern="^[0-9a-f]{64}$" |  |
| `through_revision` | yes | type="string"; minLength=1; maxLength=19; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18})$" |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `segmented_no_total_max` | maximum=100, reason=bounded-route-page |
| length | characters | `contract_max` | maximum=4096, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=19, minimum=1, reason=schema-maximum |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CatalogSyncDelete](schemas-catalogsyncdelete.md)
- [schemas: CatalogSyncUpsert](schemas-catalogsyncupsert.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/route-progression/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CatalogSyncChangePage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f9134d3884d10ad80c358ca088ef4cb1983554941cd38bf93eb6e4c22d95cd09 -->

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
    "caught_up": {
      "title": "Caught Up",
      "type": "boolean"
    },
    "changes": {
      "items": {
        "discriminator": {
          "mapping": {
            "delete": "#/components/schemas/CatalogSyncDelete",
            "upsert": "#/components/schemas/CatalogSyncUpsert"
          },
          "propertyName": "operation"
        },
        "oneOf": [
          {
            "$ref": "#/components/schemas/CatalogSyncUpsert"
          },
          {
            "$ref": "#/components/schemas/CatalogSyncDelete"
          }
        ]
      },
      "maxItems": 100,
      "title": "Changes",
      "type": "array"
    },
    "format": {
      "const": "riverhog-catalog-sync/v1",
      "default": "riverhog-catalog-sync/v1",
      "title": "Format",
      "type": "string"
    },
    "next_cursor": {
      "maxLength": 4096,
      "minLength": 1,
      "title": "Next Cursor",
      "type": "string"
    },
    "source_identity": {
      "maxLength": 64,
      "minLength": 64,
      "pattern": "^[0-9a-f]{64}$",
      "title": "Source Identity",
      "type": "string"
    },
    "through_revision": {
      "maxLength": 19,
      "minLength": 1,
      "pattern": "^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18})$",
      "title": "Through Revision",
      "type": "string"
    }
  },
  "required": [
    "source_identity",
    "authorization_view_identity",
    "changes",
    "next_cursor",
    "caught_up",
    "through_revision"
  ],
  "title": "CatalogSyncChangePage",
  "type": "object"
}
```
