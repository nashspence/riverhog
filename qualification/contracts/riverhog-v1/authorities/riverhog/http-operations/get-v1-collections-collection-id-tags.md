# GET /v1/collections/{collection_id}/tags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-collections-collection-id-tags:dfa3c428a5 -->

List Collection Tags

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-ebc23e6a3b"></a>
- <a id="s-8d52fce6b4"></a>`operationId`: list_collection_tags
- <a id="s-a99fe14e37"></a>`summary`: List Collection Tags
- <a id="s-df0b975dea"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-61d6bf74c3"></a>`collection_id` | path | yes | not declared | type="integer"; minimum=1 |
| <a id="s-d879f8ea42"></a>`revision` | query | yes | not declared | type="integer"; minimum=1 |
| <a id="s-a651f24cea"></a>`tag_set_identity` | query | yes | not declared | type="string"; pattern="^[0-9a-f]{64}$" |
| <a id="s-9d44d0e0b3"></a>`page_size` | query | no | `25` | type="integer"; minimum=1; maximum=100 |
| <a id="s-8ca4af851e"></a>`page_token` | query | no | not declared | anyOf=[BrowsePageToken](../http-schemas/schemas-browsepagetoken.md) \| type="null" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-55b4e0fcfa"></a>`200` | Successful Response | application/json | [CollectionTagListOut](../http-schemas/schemas-collectiontaglistout.md) | not declared |
| <a id="s-91b9e58165"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-04b4e54794"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-177afcce12"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-60b919b53b"></a>`404` | Not Found | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `not_found` |
| <a id="s-1968e4d755"></a>`409` | Conflict | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `conflict` |
| <a id="s-7daee174eb"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"authority":"collection-tag-set","authority_parameter":"tag_set_identity","cursor_parameter":"page_token","kind":"exact-authority-page","limit_parameter":"page_size"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/collections/{collection_id}/tags](#s-ebc23e6a3b) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-741ef68ae4"></a>[parameter page_size](#s-9d44d0e0b3) | `value · schema-value · contract_max` | maximum=100; minimum=1; reason="schema-maximum" |
| <a id="s-00e27b38e6"></a>[parameter tag_set_identity](#s-a651f24cea) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

### Progression evidence and open obligations

These are candidate test bindings. Group-wide progression claims remain unestablished; inspect the test scopes before applying a result to this contract.

- [riverhog-read-collection-progression/v1](../../../evidence/sources.md#e-5707b3a2d3-1536c4a29a)

## Maintained corroboration

### Related interface records

- [piggity collection tag list](../../piggity/cli/piggity-collection-tag-list.md)
- [piggity local add](../../piggity/cli/piggity-local-add.md)
- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)
- [riverhog_client.ApiClient.list_collection_tags](../../riverhog-client/python/riverhog-client-apiclient-list-collection-tags.md)

### Referenced contract dossiers

- [schemas: BrowsePageToken](../http-schemas/schemas-browsepagetoken.md)
- [schemas: CollectionTagListOut](../http-schemas/schemas-collectiontaglistout.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-5486674c80"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-6c4396d74a"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-4917d6da55"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **OpenAPI authority:** [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9)
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`
- **Handler:** [riverhog/src/riverhog_api/routers/tags.py::list_collection_tags](../../../../../../riverhog/src/riverhog_api/routers/tags.py#L66)

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

<details>
<summary>Exact structural binding record</summary>

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_bindings": [
    {
      "command": "collection tag list",
      "source": {
        "line": 998,
        "module": "piggity.main",
        "path": "reference/riverhog/applications/piggity/src/piggity/main.py",
        "symbol": "collection_tag_list_cmd"
      }
    },
    {
      "command": "local add",
      "source": {
        "line": 902,
        "module": "piggity.local",
        "path": "reference/riverhog/applications/piggity/src/piggity/local.py",
        "symbol": "add_collection"
      }
    },
    {
      "command": "local sync",
      "source": {
        "line": 1101,
        "module": "piggity.local",
        "path": "reference/riverhog/applications/piggity/src/piggity/local.py",
        "symbol": "sync"
      }
    },
    {
      "command": "local repair",
      "source": {
        "line": 1122,
        "module": "piggity.local",
        "path": "reference/riverhog/applications/piggity/src/piggity/local.py",
        "symbol": "repair"
      }
    }
  ],
  "cli_commands": [
    "collection tag list",
    "local add",
    "local repair",
    "local sync"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.list_collection_tags",
      "source": {
        "line": 2244,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.list_collection_tags"
      }
    }
  ],
  "method": "GET",
  "operation_id": "list_collection_tags",
  "path": "/v1/collections/{collection_id}/tags",
  "provider_evidence": null,
  "read_collection": {
    "authority": "collection-tag-set",
    "authority_parameter": "tag_set_identity",
    "cursor_parameter": "page_token",
    "kind": "exact-authority-page",
    "limit_parameter": "page_size"
  },
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collections~1{collection_id}~1tags/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1689028420bff625de6a170c88aa502bcd9a0115f77269d719af9973d06c12ec -->

```json
{
  "operationId": "list_collection_tags",
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
    },
    {
      "in": "query",
      "name": "page_size",
      "required": false,
      "schema": {
        "default": 25,
        "maximum": 100,
        "minimum": 1,
        "title": "Page Size",
        "type": "integer"
      }
    },
    {
      "in": "query",
      "name": "page_token",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "$ref": "#/components/schemas/BrowsePageToken"
          },
          {
            "type": "null"
          }
        ],
        "title": "Page Token"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/CollectionTagListOut"
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
  "summary": "List Collection Tags",
  "tags": [
    "collection-tags"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "catalog:read"
      ]
    }
  ],
  "x-riverhog-read-collection": {
    "authority": "collection-tag-set",
    "authority_parameter": "tag_set_identity",
    "cursor_parameter": "page_token",
    "kind": "exact-authority-page",
    "limit_parameter": "page_size"
  }
}
```
