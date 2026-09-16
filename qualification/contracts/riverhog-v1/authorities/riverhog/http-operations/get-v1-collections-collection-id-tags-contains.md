# GET /v1/collections/{collection_id}/tags:contains

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-collections-collection-id-tags-contains:ea19863576 -->

Collection Contains Tag

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-b7b3da9ca4"></a>
- <a id="s-f64404d5ae"></a>`operationId`: collection_contains_tag
- <a id="s-c1758ccadd"></a>`summary`: Collection Contains Tag
- <a id="s-2ba7d67bf8"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-234c3b9e4a"></a>`collection_id` | path | yes | type="integer"; minimum=1 |
| <a id="s-a9d8b50545"></a>`tag` | query | yes | #/components/schemas/CollectionTag |
| <a id="s-d314b5a767"></a>`revision` | query | yes | type="integer"; minimum=1 |
| <a id="s-2807a21da2"></a>`tag_set_identity` | query | yes | type="string"; pattern="^[0-9a-f]{64}$" |

### Responses

| Status | Description |
|---|---|
| <a id="s-02989a39c4"></a>`200` | Successful Response |
| <a id="s-9c209972c1"></a>`400` | Bad Request |
| <a id="s-33482f9ae5"></a>`401` | Unauthorized |
| <a id="s-3ea1e0e8fb"></a>`403` | Forbidden |
| <a id="s-f21bc1a6cd"></a>`404` | Not Found |
| <a id="s-3d9646a86a"></a>`409` | Conflict |
| <a id="s-2c1f136096"></a>`500` | Internal Server Error |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-0c1779b3ec"></a>[parameter tag_set_identity](#s-2807a21da2) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [piggity collection tag contains](../../piggity/cli/piggity-collection-tag-contains.md)

### Referenced contract dossiers

- [schemas: CollectionTag](../http-schemas/schemas-collectiontag.md)
- [schemas: CollectionTagMembershipOut](../http-schemas/schemas-collectiontagmembershipout.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-8915a89c73"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-0052a98669"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

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
    "collection tag contains"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "collection_contains_tag",
  "path": "/v1/collections/{collection_id}/tags:contains",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collections~1{collection_id}~1tags:contains/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: da8b7e3527367c81ff8666ccbd28aa88aa36ac7eea96ee9f88a450efc7982807 -->

```json
{
  "operationId": "collection_contains_tag",
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
      "in": "query",
      "name": "tag",
      "required": true,
      "schema": {
        "$ref": "#/components/schemas/CollectionTag"
      }
    },
    {
      "in": "query",
      "name": "revision",
      "required": true,
      "schema": {
        "minimum": 1,
        "title": "Revision",
        "type": "integer"
      }
    },
    {
      "in": "query",
      "name": "tag_set_identity",
      "required": true,
      "schema": {
        "pattern": "^[0-9a-f]{64}$",
        "title": "Tag Set Identity",
        "type": "string"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/CollectionTagMembershipOut"
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
  "summary": "Collection Contains Tag",
  "tags": [
    "collection-tags"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "catalog:read"
      ]
    }
  ]
}
```
