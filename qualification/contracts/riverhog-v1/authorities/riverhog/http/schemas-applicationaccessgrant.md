# schemas: ApplicationAccessGrant

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-applicationaccessgrant:4c6bf33b3e -->

One canonical public application-access request or response grant.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-ee1b3c97d7"></a>
- <a id="s-bdfdb13bfb"></a>`title`: ApplicationAccessGrant
- <a id="s-5f6a2a8d90"></a>`description`: One canonical public application-access request or response grant.
- <a id="s-2c00789161"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c2cd54ec86"></a>`permission` | yes | #/components/schemas/ApplicationPermission |  |
| <a id="s-ea597f207a"></a>`resource` | no | $ref="#/components/schemas/ApplicationResource" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ApplicationPermission](schemas-applicationpermission.md)
- [schemas: ApplicationResource](schemas-applicationresource.md)

## Governing policies

- <a id="pa-15bf992bf3"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ApplicationAccessGrant`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1a73b17eccc32820fbe81fb1c4cb779b83cc4fcd6728aa1f4ca5e5b8223d8484 -->

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
  "description": "One canonical public application-access request or response grant.",
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
  "title": "ApplicationAccessGrant",
  "type": "object"
}
```
