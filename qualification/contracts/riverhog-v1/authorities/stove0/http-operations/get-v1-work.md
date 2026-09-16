# GET /v1/work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:stove0:get-v1-work:8c40f32a87 -->

List Work

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-e3469ba5ea"></a>
- <a id="s-bcf1ddc968"></a>`operationId`: `"list_work"`
- <a id="s-6a02feaa41"></a>`summary`: `"List Work"`
- <a id="s-d7f1632eb4"></a>`tags`: `["work"]`
- <a id="s-d62ca9ca8c"></a>`x-riverhog-read-collection`: `{"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-f757afb8e1"></a>`page_size` | query | no | `25` | type="integer"; minimum=1; maximum=100; title="Page Size" |
| <a id="s-eb23c506e0"></a>`page_token` | query | no | not declared | anyOf=[([BrowsePageToken](../http-schemas/schemas-browsepagetoken.md)); (type="null")]; title="Page Token" |
| <a id="s-3c013fe019"></a>`phase` | query | no | not declared | anyOf=[(type="string"; enum=["eligible","claimed","observing","planning","target_preflight","queued","executing","output_finalizing","verifying","settled","retirement_pending","coordinating","abandon_pending","complete","inapplicable","failed","canceled"]); (type="null")]; title="Phase" |
| <a id="s-5f521e9cb3"></a>`q` | query | no | not declared | anyOf=[([BrowseQuery](../http-schemas/schemas-browsequery.md)); (type="null")]; title="Q" |
| <a id="s-24e7c1f77e"></a>`sort` | query | no | `"updated_at"` | type="string"; enum=["updated_at","phase","work_id"]; title="Sort" |
| <a id="s-781a235d35"></a>`order` | query | no | `"desc"` | type="string"; enum=["asc","desc"]; title="Order" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-ce11aed2b2"></a>`200` | Successful Response | application/json | [WorkPage](../http-schemas/schemas-workpage.md) | not declared |
| <a id="s-f866888f93"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-b20246c2a8"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-2d228313df"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-cd90dda77d"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/work](#s-e3469ba5ea) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-847cd29f11"></a>[parameter page_size](#s-f757afb8e1) | `value · schema-value · contract_max` | shared above |

### Progression evidence and open obligations

These are candidate test bindings. Group-wide progression claims remain unestablished; inspect the test scopes before applying a result to this contract.

- [stove0-read-collection-progression/v1](../../../evidence/sources.md#e-5707b3a2d3-34931f753b)

## Maintained corroboration

### Related interface records

- [stove0 work list](../../stove0-client/cli/stove0-work-list.md)
- [stove0_api_client.Stove0ApiClient.list_work](../../stove0-api-client/python/stove0-api-client-stove0apiclient-list-work.md)

### Referenced contract dossiers

- [schemas: BrowsePageToken](../http-schemas/schemas-browsepagetoken.md)
- [schemas: BrowseQuery](../http-schemas/schemas-browsequery.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: WorkPage](../http-schemas/schemas-workpage.md)

## Governing policies

- <a id="pa-0412e88915"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-0650c319ca"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-112321b18b"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **OpenAPI authority:** [openapi:stove0](../../../evidence/sources.md#src-52e6e32124)
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`
- **Handler:** [reference/stove0/application/server/src/stove0_api/app.py::create_app.<locals>.list_work](../../../../../../reference/stove0/application/server/src/stove0_api/app.py#L659)

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

<details>
<summary>Exact structural binding record</summary>

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_bindings": [
    {
      "command": "work list",
      "source": {
        "line": 267,
        "module": "stove0_cli.main",
        "path": "reference/stove0/application/client/src/stove0_cli/main.py",
        "symbol": "list_work"
      }
    }
  ],
  "cli_commands": [
    "work list"
  ],
  "client": "Stove0ApiClient",
  "client_bindings": [
    {
      "public_identity": "stove0_api_client.Stove0ApiClient.list_work",
      "source": {
        "line": 219,
        "module": "stove0_api_client.client",
        "path": "reference/stove0/packages/api-client/src/stove0_api_client/client.py",
        "symbol": "Stove0ApiClient.list_work"
      }
    }
  ],
  "method": "GET",
  "operation_id": "list_work",
  "path": "/v1/work",
  "provider_evidence": null,
  "read_collection": {
    "default_page_size": 25,
    "kind": "mutable-browse",
    "maximum_page_size": 100,
    "next_page_token_field": "next_page_token",
    "page_size_parameter": "page_size",
    "page_token_parameter": "page_token"
  },
  "response_authority": "operator-projection"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1work/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 761543e112dc6d7eb61039c7317c73e2583f90df0db8b1ad6e25d1cc655119dc -->

```json
{
  "operationId": "list_work",
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
      "name": "phase",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "enum": [
              "eligible",
              "claimed",
              "observing",
              "planning",
              "target_preflight",
              "queued",
              "executing",
              "output_finalizing",
              "verifying",
              "settled",
              "retirement_pending",
              "coordinating",
              "abandon_pending",
              "complete",
              "inapplicable",
              "failed",
              "canceled"
            ],
            "type": "string"
          },
          {
            "type": "null"
          }
        ],
        "title": "Phase"
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
        "default": "updated_at",
        "enum": [
          "updated_at",
          "phase",
          "work_id"
        ],
        "title": "Sort",
        "type": "string"
      }
    },
    {
      "in": "query",
      "name": "order",
      "required": false,
      "schema": {
        "default": "desc",
        "enum": [
          "asc",
          "desc"
        ],
        "title": "Order",
        "type": "string"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/WorkPage"
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
  "summary": "List Work",
  "tags": [
    "work"
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
