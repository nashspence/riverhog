# GET /v1/collections

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-collections:46ebdf5e7d -->

List Collections

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-c0edb146f5"></a>
- <a id="s-46c0cf000f"></a>`operationId`: `"list_collections"`
- <a id="s-cd13077717"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-f96134ae88"></a>`summary`: `"List Collections"`
- <a id="s-246494bf89"></a>`tags`: `["collections"]`
- <a id="s-3d317e5df0"></a>`x-riverhog-permission-requirements`: `[{"any_of":["catalog:read"]}]`
- <a id="s-04c037e6f1"></a>`x-riverhog-read-collection`: `{"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token","response_items_field":"collections"}`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-2076d85ce1"></a>`page_size` | query | no | `25` | type="integer"; minimum=1; maximum=100; title="Page Size" |
| <a id="s-9731e95241"></a>`page_token` | query | no | not declared | anyOf=[([BrowsePageToken](../http-schemas/schemas-browsepagetoken.md)); (type="null")]; title="Page Token" |
| <a id="s-2bab53adb8"></a>`q` | query | no | not declared | anyOf=[([BrowseQuery](../http-schemas/schemas-browsequery.md)); (type="null")]; title="Q" |
| <a id="s-3495833d03"></a>`sort` | query | no | `"id"` | [CollectionSort](../http-schemas/schemas-collectionsort.md) |
| <a id="s-fba33675a7"></a>`order` | query | no | `"asc"` | [SortOrder](../http-schemas/schemas-sortorder.md) |
| <a id="s-a62ca229bb"></a>`encryption_format` | query | no | not declared | anyOf=[(type="string"); (type="null")]; title="Encryption Format" |
| <a id="s-6516891cb2"></a>`passphrase_id` | query | no | not declared | anyOf=[(type="string"); (type="null")]; title="Passphrase Id" |
| <a id="s-c928d33a4d"></a>`tags` | query | no | not declared | anyOf=[(type="array"; items=([CollectionTag](../http-schemas/schemas-collectiontag.md)); maxItems=100; x-riverhog-extent={"policy":"contract_max","reason":"bounded-exact-tag-selector-batch"}); (type="null")]; title="Tags" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-64bfaf8f5a"></a>`200` | Successful Response | application/json | [ListCollectionsResponse](../http-schemas/schemas-listcollectionsresponse.md) | not declared |
| <a id="s-1a717dd59c"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-8d982699dd"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-31c81ac156"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-c6eb104848"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token","response_items_field":"collections"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/collections](#s-c0edb146f5) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-0696f31dde"></a>[parameter page_size](#s-2076d85ce1) | `value · schema-value · contract_max` | minimum=1; reason="schema-maximum" |
| <a id="s-adcb77fe2d"></a>[parameter tags · array value](#s-c928d33a4d) | `cardinality · items · contract_max` | reason="bounded-exact-tag-selector-batch" |

### Progression evidence and open obligations

These are candidate test bindings. Group-wide progression claims remain unestablished; inspect the test scopes before applying a result to this contract.

- [riverhog-read-collection-progression/v1](../../../evidence/sources.md#e-5707b3a2d3-1536c4a29a)

## Maintained corroboration

### Related interface records

- [piggity collection list](../../piggity/cli/piggity-collection-list.md)
- [riverhog_client.ApiClient.list_collections](../../riverhog-client/python/riverhog-client-apiclient-list-collections.md)

### Referenced contract dossiers

- [schemas: BrowsePageToken](../http-schemas/schemas-browsepagetoken.md)
- [schemas: BrowseQuery](../http-schemas/schemas-browsequery.md)
- [schemas: CollectionSort](../http-schemas/schemas-collectionsort.md)
- [schemas: CollectionTag](../http-schemas/schemas-collectiontag.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: ListCollectionsResponse](../http-schemas/schemas-listcollectionsresponse.md)
- [schemas: SortOrder](../http-schemas/schemas-sortorder.md)

## Governing policies

- <a id="pa-0d9445c818"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-e7a35d31a6"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-6dcddfdec9"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **OpenAPI authority:** [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9)
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`
- **Handler:** [riverhog/src/riverhog_api/routers/collections.py::list_collections](../../../../../../riverhog/src/riverhog_api/routers/collections.py#L113)

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
      "command": "collection list",
      "source": {
        "line": 2095,
        "module": "piggity.main",
        "path": "reference/riverhog/applications/piggity/src/piggity/main.py",
        "symbol": "collection_list_cmd"
      }
    }
  ],
  "cli_commands": [
    "collection list"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.list_collections",
      "source": {
        "line": 1994,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.list_collections"
      }
    }
  ],
  "method": "GET",
  "operation_id": "list_collections",
  "path": "/v1/collections",
  "provider_evidence": null,
  "read_collection": {
    "default_page_size": 25,
    "kind": "mutable-browse",
    "maximum_page_size": 100,
    "next_page_token_field": "next_page_token",
    "page_size_parameter": "page_size",
    "page_token_parameter": "page_token",
    "response_items_field": "collections"
  },
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collections/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0caad8eb530cb8908e54f1ea864372b24ec2642191eebcd2cd9368561247d55e -->

```json
{
  "operationId": "list_collections",
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
    },
    {
      "in": "query",
      "name": "sort",
      "required": false,
      "schema": {
        "$ref": "#/components/schemas/CollectionSort",
        "default": "id"
      }
    },
    {
      "in": "query",
      "name": "order",
      "required": false,
      "schema": {
        "$ref": "#/components/schemas/SortOrder",
        "default": "asc"
      }
    },
    {
      "in": "query",
      "name": "encryption_format",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "type": "string"
          },
          {
            "type": "null"
          }
        ],
        "title": "Encryption Format"
      }
    },
    {
      "in": "query",
      "name": "passphrase_id",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "type": "string"
          },
          {
            "type": "null"
          }
        ],
        "title": "Passphrase Id"
      }
    },
    {
      "in": "query",
      "name": "tags",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "items": {
              "$ref": "#/components/schemas/CollectionTag"
            },
            "maxItems": 100,
            "type": "array",
            "x-riverhog-extent": {
              "policy": "contract_max",
              "reason": "bounded-exact-tag-selector-batch"
            }
          },
          {
            "type": "null"
          }
        ],
        "title": "Tags"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ListCollectionsResponse"
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
  "summary": "List Collections",
  "tags": [
    "collections"
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
    "page_token_parameter": "page_token",
    "response_items_field": "collections"
  }
}
```

</details>
