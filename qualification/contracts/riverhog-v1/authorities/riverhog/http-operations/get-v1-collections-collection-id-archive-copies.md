# GET /v1/collections/{collection_id}/archive-copies

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-collections-collection-id-archive-copies:ecfa4362af -->

List Collection Archive Copies

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-1bb9a248a2"></a>
- <a id="s-7d155223a1"></a>`operationId`: `"list_collection_archive_copies"`
- <a id="s-0950d4f285"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-c68349e316"></a>`summary`: `"List Collection Archive Copies"`
- <a id="s-b281ff5a85"></a>`tags`: `["collections"]`
- <a id="s-fd2c1b88e8"></a>`x-riverhog-permission-requirements`: `[{"any_of":["catalog:read"]}]`
- <a id="s-d8c4f5a131"></a>`x-riverhog-read-collection`: `{"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-048e4c0841"></a>`collection_id` | path | yes | not declared | type="integer"; minimum=1; title="Collection Id" |
| <a id="s-ad146b6b78"></a>`page_size` | query | no | `25` | type="integer"; minimum=1; maximum=100; title="Page Size" |
| <a id="s-8c902e89d2"></a>`page_token` | query | no | not declared | anyOf=[([BrowsePageToken](../http-schemas/schemas-browsepagetoken.md)); (type="null")]; title="Page Token" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-f1aebea4ae"></a>`200` | Successful Response | application/json | [CollectionArchiveCopyListOut](../http-schemas/schemas-collectionarchivecopylistout.md) | not declared |
| <a id="s-8af4eca199"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-e9c6c3fbb1"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-45930e2248"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-ac2bfe9561"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/collections/{collection_id}/archive-copies](#s-1bb9a248a2) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-acad68759a"></a>[parameter page_size](#s-ad146b6b78) | `value · schema-value · contract_max` | shared above |

### Progression evidence and open obligations

These are candidate test bindings. Group-wide progression claims remain unestablished; inspect the test scopes before applying a result to this contract.

- [riverhog-read-collection-progression/v1](../../../evidence/sources.md#e-5707b3a2d3-1536c4a29a)

## Maintained corroboration

### Related interface records

- [piggity collection archive-copies](../../piggity/cli/piggity-collection-archive-copies.md)
- [riverhog_client.ApiClient.list_collection_archive_copies](../../riverhog-client/python/riverhog-client-apiclient-list-collection-archive-copies.md)

### Referenced contract dossiers

- [schemas: BrowsePageToken](../http-schemas/schemas-browsepagetoken.md)
- [schemas: CollectionArchiveCopyListOut](../http-schemas/schemas-collectionarchivecopylistout.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-a4a43ccebd"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-1a56063d3e"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-b34f41fb4d"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **OpenAPI authority:** [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9)
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`
- **Handler:** [riverhog/src/riverhog_api/routers/collections.py::list_collection_archive_copies](../../../../../../riverhog/src/riverhog_api/routers/collections.py#L166)

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
      "command": "collection archive-copies",
      "source": {
        "line": 2149,
        "module": "piggity.main",
        "path": "reference/riverhog/applications/piggity/src/piggity/main.py",
        "symbol": "collection_archive_copies_cmd"
      }
    }
  ],
  "cli_commands": [
    "collection archive-copies"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.list_collection_archive_copies",
      "source": {
        "line": 1604,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.list_collection_archive_copies"
      }
    }
  ],
  "method": "GET",
  "operation_id": "list_collection_archive_copies",
  "path": "/v1/collections/{collection_id}/archive-copies",
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

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collections~1{collection_id}~1archive-copies/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8b1c26f6427d66bd5db062d2ef649ac27bd6fa19347f0c763ce001d219072a5f -->

```json
{
  "operationId": "list_collection_archive_copies",
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
            "$ref": "#/components/schemas/CollectionArchiveCopyListOut"
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
  "summary": "List Collection Archive Copies",
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
    "page_token_parameter": "page_token"
  }
}
```

</details>
