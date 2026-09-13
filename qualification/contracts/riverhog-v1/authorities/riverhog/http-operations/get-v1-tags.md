# GET /v1/tags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-tags:44be3c5cdd -->

List Tags

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-976d133056"></a>
- <a id="s-a3931a140e"></a>`operationId`: list_tags
- <a id="s-3bfac3a172"></a>`summary`: List Tags
- <a id="s-ed24fad37e"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-25294d569d"></a>`page_size` | query | no | type="integer"; minimum=1; maximum=100 |
| <a id="s-b1e178cad7"></a>`page_token` | query | no | anyOf=#/components/schemas/BrowsePageToken \| type="null" |
| <a id="s-2269b25776"></a>`q` | query | no | anyOf=#/components/schemas/BrowseQuery \| type="null" |

### Responses

| Status | Description |
|---|---|
| <a id="s-3381dcf43b"></a>`200` | Successful Response |
| <a id="s-ae2a9c0e1c"></a>`400` | Bad Request |
| <a id="s-d1c4b3861c"></a>`401` | Unauthorized |
| <a id="s-b08e213684"></a>`403` | Forbidden |
| <a id="s-b00bdf6c45"></a>`500` | Internal Server Error |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/tags](#s-976d133056) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-86f1577f9b"></a>[parameter page_size](#s-25294d569d) | `value · schema-value · contract_max` | shared above |

## Maintained corroboration

### Related interface records

- [piggity tag list](../../piggity/cli/piggity-tag-list.md)

### Referenced contract dossiers

- [schemas: BrowsePageToken](../http-schemas/schemas-browsepagetoken.md)
- [schemas: BrowseQuery](../http-schemas/schemas-browsequery.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: TagListOut](../http-schemas/schemas-taglistout.md)

## Governing policies

- <a id="pa-b73d14686f"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-27bd42f751"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-7df0da4658"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Operation qualification evidence

This evidence proves maintained client, CLI, response-authority, and provider qualification without creating a second semantic operation.

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "tag list"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_tags",
  "path": "/v1/tags",
  "provider_evidence": null,
  "read_collection": {
    "default_page_size": 25,
    "kind": "mutable-browse",
    "maximum_page_size": 100,
    "next_page_token_field": "next_page_token",
    "page_size_parameter": "page_size",
    "page_token_parameter": "page_token"
  },
  "response_authority": "http-json"
}
```

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1tags/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b13dae6b585d7d2fa0567e6c4ea650bf116f1ae187a57c3b1cd094893817eeb8 -->

```json
{
  "operationId": "list_tags",
  "parameters": [
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
    },
    {
      "in": "query",
      "name": "q",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "$ref": "#/components/schemas/BrowseQuery"
          },
          {
            "type": "null"
          }
        ],
        "title": "Q"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/TagListOut"
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
  "summary": "List Tags",
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
    "default_page_size": 25,
    "kind": "mutable-browse",
    "maximum_page_size": 100,
    "next_page_token_field": "next_page_token",
    "page_size_parameter": "page_size",
    "page_token_parameter": "page_token"
  }
}
```
