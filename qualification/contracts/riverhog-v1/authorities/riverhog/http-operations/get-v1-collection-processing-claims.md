# GET /v1/collection-processing-claims

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-collection-processing-claims:95a0437610 -->

List Processing Claims

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-c020b12657"></a>
- <a id="s-7e81e1c32e"></a>`operationId`: `"list_processing_claims"`
- <a id="s-b7154c2973"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-e2fd121d73"></a>`summary`: `"List Processing Claims"`
- <a id="s-deeea79264"></a>`tags`: `["collection-workflows"]`
- <a id="s-28ff6af295"></a>`x-riverhog-interface`: `"client-only-primitive"`
- <a id="s-c64a114150"></a>`x-riverhog-permission-requirements`: `[{"any_of":["collection-processing:control"]}]`
- <a id="s-4f6c70446e"></a>`x-riverhog-read-collection`: `{"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-2321207667"></a>`page_size` | query | no | `25` | type="integer"; minimum=1; maximum=100; title="Page Size" |
| <a id="s-37f200ec6c"></a>`page_token` | query | no | not declared | anyOf=[([BrowsePageToken](../http-schemas/schemas-browsepagetoken.md)); (type="null")]; title="Page Token" |
| <a id="s-96a5f47301"></a>`state` | query | no | not declared | anyOf=[(type="string"; enum=["active","settled","retiring","abandoned","released"]); (type="null")]; title="State" |
| <a id="s-6308dc537d"></a>`sort` | query | no | `"updated_at"` | type="string"; enum=["created_at","updated_at","expires_at","state","work_id","execution_id"]; title="Sort" |
| <a id="s-fa342e9615"></a>`order` | query | no | `"desc"` | type="string"; enum=["asc","desc"]; title="Order" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-c7dbec4dcf"></a>`200` | Successful Response | application/json | [ProcessingClaimPageDocument](../http-schemas/schemas-processingclaimpagedocument.md) | not declared |
| <a id="s-2ab72efb00"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-69b1b770d1"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-ff02bb11b9"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-d4893dfda8"></a>`409` | Conflict | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `invalid_state` |
| <a id="s-911a4536fc"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/collection-processing-claims](#s-c020b12657) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-01a11f4824"></a>[parameter page_size](#s-2321207667) | `value · schema-value · contract_max` | shared above |

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

- [riverhog_client.ApiClient.list_processing_claims](../../riverhog-client/python/riverhog-client-apiclient-list-processing-claims.md)

### Referenced contract elements

- [schemas: BrowsePageToken](../http-schemas/schemas-browsepagetoken.md)
- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)
- [schemas: ProcessingClaimPageDocument](../http-schemas/schemas-processingclaimpagedocument.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-d48e42ef10"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-02a75d98a1"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)
- <a id="pa-97d9e4bc41"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/workflows.py::list\_processing\_claims](../../../../../../riverhog/src/riverhog_api/routers/workflows.py#L241)

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

<details>
<summary>Exact structural binding record</summary>

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_bindings": [],
  "cli_commands": [],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.list_processing_claims",
      "source": {
        "line": 237,
        "module": "riverhog_client.workflows",
        "path": "packages/riverhog-client/src/riverhog_client/workflows.py",
        "symbol": "CollectionWorkflowMethods.list_processing_claims"
      }
    }
  ],
  "method": "GET",
  "operation_id": "list_processing_claims",
  "path": "/v1/collection-processing-claims",
  "provider_evidence": null,
  "read_collection": {
    "default_page_size": 25,
    "kind": "mutable-browse",
    "maximum_page_size": 100,
    "next_page_token_field": "next_page_token",
    "page_size_parameter": "page_size",
    "page_token_parameter": "page_token"
  },
  "response_authority": "canonical-document"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-processing-claims/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c4972a1e28f8bcfa14075922d276378d132f828131942b876a948c874edd41a3 -->

```json
{
  "operationId": "list_processing_claims",
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
      "name": "state",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "enum": [
              "active",
              "settled",
              "retiring",
              "abandoned",
              "released"
            ],
            "type": "string"
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
      "name": "sort",
      "required": false,
      "schema": {
        "default": "updated_at",
        "enum": [
          "created_at",
          "updated_at",
          "expires_at",
          "state",
          "work_id",
          "execution_id"
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
            "$ref": "#/components/schemas/ProcessingClaimPageDocument"
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
    "409": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorOut"
          }
        }
      },
      "description": "Conflict",
      "x-riverhog-error-codes": [
        "invalid_state"
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
  "summary": "List Processing Claims",
  "tags": [
    "collection-workflows"
  ],
  "x-riverhog-interface": "client-only-primitive",
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "collection-processing:control"
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
