# POST /v1/collection-upload-sessions/{collection_id}/discard-plan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:post-v1-collection-upload-sessions-collec-793cc2515d:91011b9184 -->

Plan Collection Upload Discard

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-f9960d3305"></a>
- <a id="s-267825dd11"></a>`operationId`: plan_collection_upload_discard
- <a id="s-1db267fc9f"></a>`summary`: Plan Collection Upload Discard
- <a id="s-bbbfb609cd"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-d31d70c952"></a>`collection_id` | path | yes | type="integer"; minimum=1 |

### Responses

| Status | Description |
|---|---|
| <a id="s-ee09029c45"></a>`200` | Successful Response |
| <a id="s-fad7a886d0"></a>`400` | Bad Request |
| <a id="s-7ef61a079d"></a>`401` | Unauthorized |
| <a id="s-268bf9024a"></a>`403` | Forbidden |
| <a id="s-d784170a16"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [piggity collection upload discard](../../piggity/cli/piggity-collection-upload-discard.md)

### Referenced contract dossiers

- [schemas: CollectionUploadDiscardPlanOut](../http-schemas/schemas-collectionuploaddiscardplanout.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-f4dc40c0c2"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection upload discard"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "plan_collection_upload_discard",
  "path": "/v1/collection-upload-sessions/{collection_id}/discard-plan",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-upload-sessions~1{collection_id}~1discard-plan/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8d49d44d3d9d9f96e738fa6e95148adacc21f8b865a3d69c8440fe2d6649b9a6 -->

```json
{
  "operationId": "plan_collection_upload_discard",
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
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/CollectionUploadDiscardPlanOut"
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
  "summary": "Plan Collection Upload Discard",
  "tags": [
    "collections"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "collections:delete"
      ]
    }
  ]
}
```
