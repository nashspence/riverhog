# GET /v1/archive/stores/{store}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:get-v1-archive-stores-store:c92b41278f -->

Get Archive Store

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [archive](families/archive/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-54ed75001a"></a>
- <a id="s-e0da451e48"></a>`operationId`: get_archive_store
- <a id="s-cf1105a3ac"></a>`summary`: Get Archive Store
- <a id="s-0ebb68c0e7"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-7b748f31c6"></a>`store` | path | yes | type="string"; pattern="^[a-z0-9]+(?:-[a-z0-9]+)*$" |

### Responses

| Status | Description |
|---|---|
| <a id="s-0c14d5abd4"></a>`200` | Successful Response |
| <a id="s-d77497a05a"></a>`400` | Bad Request |
| <a id="s-40f929e655"></a>`401` | Unauthorized |
| <a id="s-ca9e03ec21"></a>`403` | Forbidden |
| <a id="s-848bef10e0"></a>`404` | Not Found |
| <a id="s-d42d32e2d5"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: get_archive_store](../operation/operation-parity-get-archive-store.md)

### Referenced contract dossiers

- [schemas: ArchiveStoreOut](schemas-archivestoreout.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)

## Governing policies

- <a id="pa-b6ac3e08dd"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1archive~1stores~1{store}/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cb289346066d8e13fe28cba0d7472e645952e12b1b2f5851e213642bb703e75b -->

```json
{
  "operationId": "get_archive_store",
  "parameters": [
    {
      "in": "path",
      "name": "store",
      "required": true,
      "schema": {
        "pattern": "^[a-z0-9]+(?:-[a-z0-9]+)*$",
        "title": "Store",
        "type": "string"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ArchiveStoreOut"
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
  "summary": "Get Archive Store",
  "tags": [
    "archive"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "archives:read"
      ]
    }
  ]
}
```
