# schemas: AppAccessListFiltersOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-appaccesslistfiltersout:80c05b15d7 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/AppAccessListFiltersOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ApplicationKeyId](schemas-applicationkeyid.md)
- [schemas: ApplicationName](schemas-applicationname.md)
- [schemas: ApplicationPermission](schemas-applicationpermission.md)
- [schemas: ApplicationResource](schemas-applicationresource.md)

## Contract summary

- `title`: AppAccessListFiltersOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `active` | yes | object (2 fields) |  |
| `app` | yes | object (1 fields) |  |
| `key_id` | yes | object (1 fields) |  |
| `permission` | yes | object (1 fields) |  |
| `resource` | yes | object (1 fields) |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 58c3b0affb66a528725d95691c3bcf296127dcbceab2530c96991cf42714fb86 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "active": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "title": "Active"
    },
    "app": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ApplicationName"
        },
        {
          "type": "null"
        }
      ]
    },
    "key_id": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ApplicationKeyId"
        },
        {
          "type": "null"
        }
      ]
    },
    "permission": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ApplicationPermission"
        },
        {
          "type": "null"
        }
      ]
    },
    "resource": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ApplicationResource"
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "app",
    "key_id",
    "permission",
    "resource",
    "active"
  ],
  "title": "AppAccessListFiltersOut",
  "type": "object"
}
```
