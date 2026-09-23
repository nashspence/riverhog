# GET /v1/retrieval-cache/objects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-retrieval-cache-objects:341500ca2e -->

List Retrieval Cache Objects

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-8d492ab675"></a>
- <a id="s-b7fc6d3007"></a>`operationId`: `"list_retrieval_cache_objects"`
- <a id="s-8df797da78"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-88b74e3e7d"></a>`summary`: `"List Retrieval Cache Objects"`
- <a id="s-76bac958c9"></a>`tags`: `["retrieval"]`
- <a id="s-10eb812eaf"></a>`x-riverhog-permission-requirements`: `[{"any_of":["catalog:read"]}]`
- <a id="s-e9d98f2732"></a>`x-riverhog-read-collection`: `{"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-e92cbb3fea"></a>`page_size` | query | no | `25` | type="integer"; minimum=1; maximum=100; title="Page Size" |
| <a id="s-dd0f2add97"></a>`page_token` | query | no | not declared | anyOf=[([BrowsePageToken](../http-schemas/schemas-browsepagetoken.md)); (type="null")]; title="Page Token" |
| <a id="s-aded9a430c"></a>`q` | query | no | not declared | anyOf=[([BrowseQuery](../http-schemas/schemas-browsequery.md)); (type="null")]; title="Q" |
| <a id="s-c8add90ad7"></a>`collection_id` | query | no | not declared | anyOf=[([CollectionIdParameter](../http-schemas/schemas-collectionidparameter.md)); (type="null")]; title="Collection Id" |
| <a id="s-a9e9c9b389"></a>`source_store` | query | no | not declared | anyOf=[([ArchiveStoreName](../http-schemas/schemas-archivestorename.md)); (type="null")]; title="Source Store" |
| <a id="s-1f2a183ae9"></a>`cache_store` | query | no | not declared | anyOf=[([RetrievalCacheStoreName](../http-schemas/schemas-retrievalcachestorename.md)); (type="null")]; title="Cache Store" |
| <a id="s-278a2451e8"></a>`state` | query | no | not declared | anyOf=[([RetrievalCacheState](../http-schemas/schemas-retrievalcachestate.md)); (type="null")]; title="State" |
| <a id="s-2cdb46627a"></a>`protection` | query | no | not declared | anyOf=[([RetrievalCacheProtection](../http-schemas/schemas-retrievalcacheprotection.md)); (type="null")]; title="Protection" |
| <a id="s-09dfc97256"></a>`expires_before` | query | no | not declared | anyOf=[(type="string"); (type="null")]; title="Expires Before" |
| <a id="s-bcbff18e3a"></a>`expires_after` | query | no | not declared | anyOf=[(type="string"); (type="null")]; title="Expires After" |
| <a id="s-3f24af8c90"></a>`sort` | query | no | `"cached_at"` | [RetrievalCacheSort](../http-schemas/schemas-retrievalcachesort.md) |
| <a id="s-2e8818cb8a"></a>`order` | query | no | `"desc"` | [SortOrder](../http-schemas/schemas-sortorder.md) |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-5f7e7e3a7a"></a>`200` | Successful Response | application/json | [RetrievalCacheObjectListOut](../http-schemas/schemas-retrievalcacheobjectlistout.md) | not declared |
| <a id="s-b3e35d547b"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-c9c2c99357"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-e4b39fdf4e"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-53f3f02986"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/retrieval-cache/objects](#s-8d492ab675) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-263795c079"></a>[parameter page_size](#s-e92cbb3fea) | `value · schema-value · contract_max` | shared above |

### Evidence gaps

The named contract groups have recorded evidence gaps in the following guarantees. Each group's page identifies its exact open guarantees and candidate tests:

- Each step stays within its declared limits.
- Continuing the work makes progress toward its declared completion.
- The operation works across multiple pages or chunks.
- Required data or work is not silently left out.
- Work can resume after a restart as its contract requires.

These guarantees let large tasks proceed in smaller steps: a limit on one page or chunk must not become a hidden limit on the whole task. Returning a first page correctly does not establish that continuation or recovery works. Capacity may explicitly reject, defer, or throttle work; it must not silently omit work.

Existing tests may establish individual cases. The gaps retain their recorded group-wide scope and do not establish a bug in every linked contract. Completion follows each contract's rules; mutable browsing carries no implied snapshot guarantee.

Required by: [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb).

Exact evidence groups for this contract element:

- [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md)

## Maintained corroboration

### Related interface records

- [piggity retrieval cache list](../../piggity/cli/piggity-retrieval-cache-list.md)
- [riverhog_client.ApiClient.list_retrieval_cache_objects](../../riverhog-client/python/riverhog-client-apiclient-list-retrieval-cache-objects.md)

### Referenced contract elements

- [schemas: ArchiveStoreName](../http-schemas/schemas-archivestorename.md)
- [schemas: BrowsePageToken](../http-schemas/schemas-browsepagetoken.md)
- [schemas: BrowseQuery](../http-schemas/schemas-browsequery.md)
- [schemas: CollectionIdParameter](../http-schemas/schemas-collectionidparameter.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: RetrievalCacheObjectListOut](../http-schemas/schemas-retrievalcacheobjectlistout.md)
- [schemas: RetrievalCacheProtection](../http-schemas/schemas-retrievalcacheprotection.md)
- [schemas: RetrievalCacheSort](../http-schemas/schemas-retrievalcachesort.md)
- [schemas: RetrievalCacheState](../http-schemas/schemas-retrievalcachestate.md)
- [schemas: RetrievalCacheStoreName](../http-schemas/schemas-retrievalcachestorename.md)
- [schemas: SortOrder](../http-schemas/schemas-sortorder.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-3b5af8a2c1"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-d4712ed873"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)
- <a id="pa-e23779f9f9"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/retrieval.py::list\_retrieval\_cache\_objects](../../../../../../riverhog/src/riverhog_api/routers/retrieval.py#L66)

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
      "command": "retrieval cache list",
      "executable": "piggity",
      "result_identity": "piggity-cli-result/retrieval/cache/list/v1",
      "source": {
        "line": 2777,
        "module": "piggity.main",
        "path": "reference/riverhog/applications/piggity/src/piggity/main.py",
        "symbol": "retrieval_cache_list_cmd"
      }
    }
  ],
  "cli_commands": [
    "retrieval cache list"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.list_retrieval_cache_objects",
      "source": {
        "line": 901,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.list_retrieval_cache_objects"
      }
    }
  ],
  "method": "GET",
  "operation_id": "list_retrieval_cache_objects",
  "path": "/v1/retrieval-cache/objects",
  "provider_evidence": "provider-qualification:#442",
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

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1retrieval-cache~1objects/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 02c7eb4735c4b8d8d9333ff27032238a27390a843c794a60d4d4647cd399f0a9 -->

```json
{
  "operationId": "list_retrieval_cache_objects",
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
      "name": "collection_id",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "$ref": "#/components/schemas/CollectionIdParameter"
          },
          {
            "type": "null"
          }
        ],
        "title": "Collection Id"
      }
    },
    {
      "in": "query",
      "name": "source_store",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "$ref": "#/components/schemas/ArchiveStoreName"
          },
          {
            "type": "null"
          }
        ],
        "title": "Source Store"
      }
    },
    {
      "in": "query",
      "name": "cache_store",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "$ref": "#/components/schemas/RetrievalCacheStoreName"
          },
          {
            "type": "null"
          }
        ],
        "title": "Cache Store"
      }
    },
    {
      "in": "query",
      "name": "state",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "$ref": "#/components/schemas/RetrievalCacheState"
          },
          {
            "type": "null"
          }
        ],
        "title": "State"
      }
    },
    {
      "in": "query",
      "name": "protection",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "$ref": "#/components/schemas/RetrievalCacheProtection"
          },
          {
            "type": "null"
          }
        ],
        "title": "Protection"
      }
    },
    {
      "in": "query",
      "name": "expires_before",
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
        "title": "Expires Before"
      }
    },
    {
      "in": "query",
      "name": "expires_after",
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
        "title": "Expires After"
      }
    },
    {
      "in": "query",
      "name": "sort",
      "required": false,
      "schema": {
        "$ref": "#/components/schemas/RetrievalCacheSort",
        "default": "cached_at"
      }
    },
    {
      "in": "query",
      "name": "order",
      "required": false,
      "schema": {
        "$ref": "#/components/schemas/SortOrder",
        "default": "desc"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/RetrievalCacheObjectListOut"
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
  "summary": "List Retrieval Cache Objects",
  "tags": [
    "retrieval"
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

</details>
