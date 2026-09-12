# POST /v1/collections/{collection_id}/tags:remove

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:post-v1-collections-collection-id-tags-remove:f6a36f4a79 -->

Remove Collection Tag

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [collections](families/collections/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-6c0b51a8a2"></a>
- <a id="s-4790c648e6"></a>`operationId`: remove_collection_tag
- <a id="s-8c37cb6586"></a>`summary`: Remove Collection Tag
- <a id="s-2e2950b25e"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-db3db1097e"></a>`collection_id` | path | yes | type="integer"; minimum=1 |

### <a id="s-3fdde7ccbe"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/CollectionTagMutationRequest"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-ea545b124d"></a>`200` | Successful Response |
| <a id="s-a1ead4ff81"></a>`400` | Bad Request |
| <a id="s-ad23e293e1"></a>`401` | Unauthorized |
| <a id="s-6a5b3501b7"></a>`403` | Forbidden |
| <a id="s-1aeecb146a"></a>`404` | Not Found |
| <a id="s-edc265bca6"></a>`409` | Conflict |
| <a id="s-40640ad73b"></a>`412` | Precondition Failed |
| <a id="s-6277d3c7be"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: remove_collection_tag](../operation/operation-parity-remove-collection-tag.md)

### Referenced contract dossiers

- [schemas: CollectionTagMutationOut](schemas-collectiontagmutationout.md)
- [schemas: CollectionTagMutationRequest](schemas-collectiontagmutationrequest.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)

## Governing policies

- <a id="pa-f324efaaee"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collections~1{collection_id}~1tags:remove/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 858969ed4ce4a5f9c375a99353a7a221a1b3c5a9f90364788061ae2d12330907 -->

```json
{
  "operationId": "remove_collection_tag",
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
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/CollectionTagMutationRequest"
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
            "$ref": "#/components/schemas/CollectionTagMutationOut"
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
    "412": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Precondition Failed",
      "x-riverhog-error-codes": [
        "precondition_failed"
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
  "summary": "Remove Collection Tag",
  "tags": [
    "collection-tags"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "collection-tags:manage"
      ]
    }
  ]
}
```
