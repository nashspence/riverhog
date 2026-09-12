# schemas: AppAccessListFiltersOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-appaccesslistfiltersout:80c05b15d7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: AppAccessListFiltersOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `active` | yes | anyOf=type="boolean" \| type="null" |  |
| `app` | yes | anyOf=#/components/schemas/ApplicationName \| type="null" |  |
| `key_id` | yes | anyOf=#/components/schemas/ApplicationKeyId \| type="null" |  |
| `permission` | yes | anyOf=#/components/schemas/ApplicationPermission \| type="null" |  |
| `resource` | yes | anyOf=#/components/schemas/ApplicationResource \| type="null" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ApplicationKeyId](schemas-applicationkeyid.md)
- [schemas: ApplicationName](schemas-applicationname.md)
- [schemas: ApplicationPermission](schemas-applicationpermission.md)
- [schemas: ApplicationResource](schemas-applicationresource.md)

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

- `/external_contract/http_openapi/riverhog/components/schemas/AppAccessListFiltersOut`

### Exact owned JSON

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
