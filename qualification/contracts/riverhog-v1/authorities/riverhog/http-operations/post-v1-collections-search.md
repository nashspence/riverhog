# POST /v1/collections:search

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:post-v1-collections-search:82716c1f16 -->

List Collections

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-c2255d3bfd"></a>
- <a id="s-420ec6ddf0"></a>`operationId`: `"list_collections"`
- <a id="s-612c550055"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-645a12b0a4"></a>`summary`: `"List Collections"`
- <a id="s-a2c3b9359b"></a>`tags`: `["collections"]`
- <a id="s-361a272afd"></a>`x-riverhog-permission-requirements`: `[{"any_of":["catalog:read"]}]`
- <a id="s-41b197d8c7"></a>`x-riverhog-read-collection`: `{"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token","response_items_field":"collections"}`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-c61018c221"></a>`page_size` | query | no | `25` | type="integer"; minimum=1; maximum=100; title="Page Size" |
| <a id="s-a0a50b2d87"></a>`page_token` | query | no | not declared | anyOf=[([BrowsePageToken](../http-schemas/schemas-browsepagetoken.md)); (type="null")]; title="Page Token" |
| <a id="s-3beafe8da4"></a>`q` | query | no | not declared | anyOf=[([BrowseQuery](../http-schemas/schemas-browsequery.md)); (type="null")]; title="Q" |
| <a id="s-733e023d4d"></a>`sort` | query | no | `"id"` | [CollectionSort](../http-schemas/schemas-collectionsort.md) |
| <a id="s-aad4f289c7"></a>`order` | query | no | `"asc"` | [SortOrder](../http-schemas/schemas-sortorder.md) |
| <a id="s-10fad7284a"></a>`encryption_format` | query | no | not declared | anyOf=[(type="string"); (type="null")]; title="Encryption Format" |
| <a id="s-dd906ab6a5"></a>`passphrase_id` | query | no | not declared | anyOf=[(type="string"); (type="null")]; title="Passphrase Id" |

### <a id="s-3f36ba9778"></a>Request body

- `required`: `true`

| Media type | Schema |
|---|---|
| application/json | [SearchCollectionsRequest](../http-schemas/schemas-searchcollectionsrequest.md) |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-578e72ed26"></a>`200` | Successful Response | application/json | [ListCollectionsOut](../http-schemas/schemas-listcollectionsout.md) | not declared |
| <a id="s-ddaee2fea9"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-906e57fb05"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-09e4ba84f9"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-8dfc29711b"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token","response_items_field":"collections"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [POST /v1/collections:search](#s-c2255d3bfd) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-087ce6e3a6"></a>[parameter page_size](#s-c61018c221) | `value · schema-value · contract_max` | shared above |

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

- [a-riverhog-cli collection list](../../a-riverhog-cli/cli/a-riverhog-cli-collection-list.md)
- [riverhog_client.ApiClient.list_collections](../../riverhog-client/python/riverhog-client-apiclient-list-collections.md)

### Referenced contract elements

- [schemas: BrowsePageToken](../http-schemas/schemas-browsepagetoken.md)
- [schemas: BrowseQuery](../http-schemas/schemas-browsequery.md)
- [schemas: CollectionSort](../http-schemas/schemas-collectionsort.md)
- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)
- [schemas: ListCollectionsOut](../http-schemas/schemas-listcollectionsout.md)
- [schemas: SearchCollectionsRequest](../http-schemas/schemas-searchcollectionsrequest.md)
- [schemas: SortOrder](../http-schemas/schemas-sortorder.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-0019b87788"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-e85340dccb"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)
- <a id="pa-97d646a9fa"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/collections.py::list\_collections](../../../../../../riverhog/src/riverhog_api/routers/collections.py#L115)

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
      "executable": "a-riverhog-cli",
      "result_identity": "a-riverhog-cli-result/collection/list/v1",
      "source": {
        "line": 2122,
        "module": "a_riverhog_cli.main",
        "path": "some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py",
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
        "line": 2022,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.list_collections"
      }
    }
  ],
  "method": "POST",
  "operation_id": "list_collections",
  "path": "/v1/collections:search",
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

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collections:search/post`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5ab14370d6675663120125da1d860ea3f4b306d066151846f7dc5fe43e08acac -->

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
    }
  ],
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/SearchCollectionsRequest"
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
            "$ref": "#/components/schemas/ListCollectionsOut"
          }
        }
      },
      "description": "Successful Response"
    },
    "400": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorOut"
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
            "$ref": "#/components/schemas/ErrorOut"
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
            "$ref": "#/components/schemas/ErrorOut"
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
            "$ref": "#/components/schemas/ErrorOut"
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
