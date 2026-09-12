# POST /v1/workflow-previews

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:post-v1-workflow-previews:53471327d9 -->

Preview Workflow

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [workflow-previews](families/workflow-previews/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-fc212e373e"></a>
- <a id="s-187346c16a"></a>`operationId`: preview_workflow
- <a id="s-4cfad3f8a3"></a>`summary`: Preview Workflow

### <a id="s-a9a0924a6f"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/WorkflowPreviewIn"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-9a578ffce8"></a>`200` | Successful Response |
| <a id="s-9654b11c29"></a>`400` | Bad Request |
| <a id="s-8d8b02934d"></a>`401` | Unauthorized |
| <a id="s-6df672a261"></a>`403` | Forbidden |
| <a id="s-a7a8b1cf22"></a>`404` | Not Found |
| <a id="s-de214b2a39"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: preview_workflow](../operation/operation-parity-preview-workflow.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: WorkflowPreview](schemas-workflowpreview.md)
- [schemas: WorkflowPreviewIn](schemas-workflowpreviewin.md)

## Governing policies

- <a id="pa-2e72602c84"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1workflow-previews/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2bd6bf55143cb3633bab8d5737fd546ea576021ca8d4032b5e3eade795111e6d -->

```json
{
  "operationId": "preview_workflow",
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/WorkflowPreviewIn"
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
            "$ref": "#/components/schemas/WorkflowPreview"
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
  "summary": "Preview Workflow",
  "tags": [
    "previews"
  ]
}
```
