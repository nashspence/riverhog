# schemas: MutateAppAccessRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-mutateappaccessrequest:2b8902f3c6 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/MutateAppAccessRequest`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ApplicationPermission](schemas-applicationpermission.md)
- [schemas: ApplicationResource](schemas-applicationresource.md)

## Contract summary

- `title`: MutateAppAccessRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `permission` | yes | #/components/schemas/ApplicationPermission |  |
| `resource` | no | #/components/schemas/ApplicationResource |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 638f68b00b9e16531f633fb81b04206fcfe4916699fb4b3adb55db52dc751d79 -->

```json
{
  "additionalProperties": false,
  "allOf": [
    {
      "oneOf": [
        {
          "properties": {
            "permission": {
              "const": "*"
            },
            "resource": {
              "const": "*"
            }
          },
          "required": [
            "permission"
          ]
        },
        {
          "properties": {
            "permission": {
              "const": "collections:create"
            },
            "resource": {
              "pattern": "^(?:\\*|tag:.+)$",
              "type": "string"
            }
          },
          "required": [
            "permission"
          ]
        },
        {
          "properties": {
            "permission": {
              "enum": [
                "archives:manage",
                "archives:read",
                "catalog:read",
                "collection-descriptions:manage",
                "collection-tags:manage",
                "collections:delete",
                "provenance:export",
                "provenance:read",
                "retrieval:manage"
              ]
            },
            "resource": {
              "pattern": "^(?:\\*|tag:.+|collection:[1-9][0-9]*)$",
              "type": "string"
            }
          },
          "required": [
            "permission"
          ]
        },
        {
          "properties": {
            "permission": {
              "enum": [
                "collection-transforms:control",
                "collection-transforms:execute",
                "events:read",
                "events:read_all",
                "keys:manage",
                "quotas:manage"
              ]
            },
            "resource": {
              "const": "*"
            }
          },
          "required": [
            "permission"
          ]
        }
      ]
    }
  ],
  "properties": {
    "permission": {
      "$ref": "#/components/schemas/ApplicationPermission"
    },
    "resource": {
      "$ref": "#/components/schemas/ApplicationResource",
      "default": "*"
    }
  },
  "required": [
    "permission"
  ],
  "title": "MutateAppAccessRequest",
  "type": "object"
}
```
