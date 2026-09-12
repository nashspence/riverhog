# POST /v1/collections/{collection_id}/tags:add

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:post-v1-collections-collection-id-tags-add:6bf0562410 -->

Add Collection Tag

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [collections](families/collections/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-0f07299467"></a>
- <a id="s-0293a9e51d"></a>`operationId`: add_collection_tag
- <a id="s-67fda963fd"></a>`summary`: Add Collection Tag
- <a id="s-0a22a69f35"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-8ea57df8d0"></a>`collection_id` | path | yes | type="integer"; minimum=1 |

### <a id="s-4543d27edd"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/CollectionTagMutationRequest"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-293ae74875"></a>`200` | Successful Response |
| <a id="s-bd782dd9ae"></a>`400` | Bad Request |
| <a id="s-34df859566"></a>`401` | Unauthorized |
| <a id="s-d1c114371c"></a>`403` | Forbidden |
| <a id="s-7bf0f4f631"></a>`404` | Not Found |
| <a id="s-e6a8959322"></a>`409` | Conflict |
| <a id="s-d2053e05da"></a>`412` | Precondition Failed |
| <a id="s-6e8ee18cc7"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: add_collection_tag](../operation/operation-parity-add-collection-tag.md)

### Referenced contract dossiers

- [schemas: CollectionTagMutationOut](schemas-collectiontagmutationout.md)
- [schemas: CollectionTagMutationRequest](schemas-collectiontagmutationrequest.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)

## Governing policies

- <a id="pa-781bf17050"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collections~1{collection_id}~1tags:add/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5ec30be8f37f5b5ef0c76628c985d20c78bf2b9428cb4dfd3d6ed53e80198291 -->

```json
{
  "operationId": "add_collection_tag",
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
  "summary": "Add Collection Tag",
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
