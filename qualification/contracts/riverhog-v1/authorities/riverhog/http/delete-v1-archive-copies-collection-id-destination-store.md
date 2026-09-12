# DELETE /v1/archive/copies/{collection_id}/{destination_store}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:delete-v1-archive-copies-collection-id-de-d504ae8621:13b59e4e67 -->

Cancel Archive Copy Job

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [archive](families/archive/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-6678a8d83e"></a>
- <a id="s-9c493bed58"></a>`operationId`: cancel_archive_copy_job
- <a id="s-60d644acc9"></a>`summary`: Cancel Archive Copy Job
- <a id="s-772a5c8e5d"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-c86344a94b"></a>`collection_id` | path | yes | type="integer"; minimum=1 |
| <a id="s-77db46652b"></a>`destination_store` | path | yes | type="string"; pattern="^[a-z0-9]+(?:-[a-z0-9]+)*$" |

### Responses

| Status | Description |
|---|---|
| <a id="s-a26bbed67a"></a>`200` | Successful Response |
| <a id="s-91784f4dd6"></a>`400` | Bad Request |
| <a id="s-fe5b6f3432"></a>`401` | Unauthorized |
| <a id="s-916f8e7812"></a>`403` | Forbidden |
| <a id="s-bd7dec8a85"></a>`404` | Not Found |
| <a id="s-4bff21ec64"></a>`409` | Conflict |
| <a id="s-8a864586a0"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: cancel_archive_copy_job](../operation/operation-parity-cancel-archive-copy-job.md)

### Referenced contract dossiers

- [schemas: ArchiveCopyJobOut](schemas-archivecopyjobout.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)

## Governing policies

- <a id="pa-c5b19aeec5"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1archive~1copies~1{collection_id}~1{destination_store}/delete`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8bc7f8b07225b838ad5ca6317d6f9522185196580144746cb3ac12cbd047875b -->

```json
{
  "operationId": "cancel_archive_copy_job",
  "parameters": [
    {
      "in": "path",
      "name": "collection_id",
      "required": true,
      "schema": {
        "minimum": 1,
        "title": "Collection Id",
        "type": "integer"
      }
    },
    {
      "in": "path",
      "name": "destination_store",
      "required": true,
      "schema": {
        "pattern": "^[a-z0-9]+(?:-[a-z0-9]+)*$",
        "title": "Destination Store",
        "type": "string"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ArchiveCopyJobOut"
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
        "invalid_state"
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
  "summary": "Cancel Archive Copy Job",
  "tags": [
    "archive"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "archives:manage"
      ]
    }
  ]
}
```
