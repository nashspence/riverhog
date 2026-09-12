# POST /v1/apps/{app}/keys/{key_id}/access

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:post-v1-apps-app-keys-key-id-access:b38bb1209b -->

Add App Key Access

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [apps](families/apps/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-4f11ed8802"></a>
- <a id="s-d9bdbbb685"></a>`operationId`: add_app_key_access
- <a id="s-8b65ebef16"></a>`summary`: Add App Key Access
- <a id="s-9583721fc7"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-63893cdb93"></a>`app` | path | yes | type="string"; pattern="^[a-z0-9]+(?:-[a-z0-9]+)*$" |
| <a id="s-08375718d6"></a>`key_id` | path | yes | type="string"; pattern="^[0-9a-f]{16}$" |

### <a id="s-e7ba4e90d6"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/MutateAppAccessRequest"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-f1906aba47"></a>`200` | Successful Response |
| <a id="s-d51e8deadd"></a>`400` | Bad Request |
| <a id="s-70b7679316"></a>`401` | Unauthorized |
| <a id="s-23814ad55b"></a>`403` | Forbidden |
| <a id="s-564a26372f"></a>`404` | Not Found |
| <a id="s-ab82137c69"></a>`409` | Conflict |
| <a id="s-4b5155e96b"></a>`500` | Internal Server Error |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=16; minimum=16; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{16}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-96e23c6645"></a>[parameter key_id](#s-08375718d6) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [Operation parity: add_app_key_access](../operation/operation-parity-add-app-key-access.md)

### Referenced contract dossiers

- [schemas: AppAccessSetOut](schemas-appaccesssetout.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: MutateAppAccessRequest](schemas-mutateappaccessrequest.md)

## Governing policies

- <a id="pa-082ee65deb"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-3868338b6a"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1apps~1{app}~1keys~1{key_id}~1access/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6bba70129ed07ef7edaa251a1aa5b788e000771bcc198e31ee301cfe12cfc776 -->

```json
{
  "operationId": "add_app_key_access",
  "parameters": [
    {
      "in": "path",
      "name": "app",
      "required": true,
      "schema": {
        "pattern": "^[a-z0-9]+(?:-[a-z0-9]+)*$",
        "title": "App",
        "type": "string"
      }
    },
    {
      "in": "path",
      "name": "key_id",
      "required": true,
      "schema": {
        "pattern": "^[0-9a-f]{16}$",
        "title": "Key Id",
        "type": "string"
      }
    }
  ],
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/MutateAppAccessRequest"
        }
      }
    },
    "required": true
  },
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/AppAccessSetOut"
          }
        }
      },
      "description": "Successful Response"
    },
    "400": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Bad Request",
      "x-riverhog-error-codes": [
        "bad_request"
      ]
    },
    "401": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Unauthorized",
      "x-riverhog-error-codes": [
        "unauthorized"
      ]
    },
    "403": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Forbidden",
      "x-riverhog-error-codes": [
        "forbidden"
      ]
    },
    "404": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Not Found",
      "x-riverhog-error-codes": [
        "not_found"
      ]
    },
    "409": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Conflict",
      "x-riverhog-error-codes": [
        "conflict"
      ]
    },
    "500": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Internal Server Error",
      "x-riverhog-error-codes": [
        "internal_error"
      ]
    }
  },
  "security": [
    {
      "HTTPBearer": []
    }
  ],
  "summary": "Add App Key Access",
  "tags": [
    "apps"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "keys:manage"
      ]
    }
  ]
}
```
