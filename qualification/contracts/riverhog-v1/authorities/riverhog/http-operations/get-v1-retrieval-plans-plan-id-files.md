# GET /v1/retrieval-plans/{plan_id}/files

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-retrieval-plans-plan-id-files:a38c19ca4d -->

List Retrieval Plan Files

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-02b9ee2a20"></a>
- <a id="s-f5a7d6ee6a"></a>`operationId`: `"list_retrieval_plan_files"`
- <a id="s-80e7646656"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-864d6dee41"></a>`summary`: `"List Retrieval Plan Files"`
- <a id="s-118cc91dae"></a>`tags`: `["retrieval"]`
- <a id="s-3bc1f9a70b"></a>`x-riverhog-interface`: `"client-only-primitive"`
- <a id="s-aa0171392e"></a>`x-riverhog-permission-requirements`: `[{"any_of":["retrieval:manage"]}]`
- <a id="s-40a8127b2a"></a>`x-riverhog-read-collection`: `{"authority":"retrieval-plan-files","cursor_parameter":"start_ordinal","kind":"exact-authority-page","limit_parameter":"page_size"}`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-f9f5dd3ea1"></a>`plan_id` | path | yes | not declared | type="string"; title="Plan Id" |
| <a id="s-b7b9300a2d"></a>`start_ordinal` | query | no | `0` | type="integer"; minimum=0; maximum=10000; title="Start Ordinal" |
| <a id="s-ee956f2b24"></a>`page_size` | query | no | `100` | type="integer"; minimum=1; maximum=100; title="Page Size" |
| <a id="s-44a185df9a"></a>`If-Match` | header | yes | not declared | type="string"; pattern="^\"[0-9a-f]{64}\"$"; title="If-Match" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-85f358c0bc"></a>`200` | Successful Response | application/json | [RetrievalPlanFilePageOut](../http-schemas/schemas-retrievalplanfilepageout.md) | not declared |
| <a id="s-7badd60fa1"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-9015f8bd0a"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-06ec61c8b6"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-99f3ea6246"></a>`404` | Not Found | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `not_found` |
| <a id="s-1ef560b954"></a>`409` | Conflict | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `invalid_state` |
| <a id="s-df924e03b9"></a>`412` | Precondition Failed | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `precondition_failed` |
| <a id="s-533383f4a4"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: progression={"authority":"retrieval-plan-files","cursor_parameter":"start_ordinal","kind":"exact-authority-page","limit_parameter":"page_size"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/retrieval-plans/{plan_id}/files](#s-02b9ee2a20) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-a7a670c975"></a>[parameter page_size](#s-ee956f2b24) | `value · schema-value · contract_max` | maximum=100; minimum=1 |
| <a id="s-3cb9b8fbc9"></a>[parameter start_ordinal](#s-b7b9300a2d) | `value · schema-value · contract_max` | maximum=10000; minimum=0 |

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

- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)
- [riverhog_client.ApiClient.list_retrieval_plan_files](../../riverhog-client/python/riverhog-client-apiclient-list-retrieval-plan-files.md)

### Referenced contract elements

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: RetrievalPlanFilePageOut](../http-schemas/schemas-retrievalplanfilepageout.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-939a9d10da"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-66302080b8"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)
- <a id="pa-00c110de3d"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/retrieval.py::list\_retrieval\_plan\_files](../../../../../../riverhog/src/riverhog_api/routers/retrieval.py#L212)

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

<details>
<summary>Exact structural binding record</summary>

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_bindings": [
    {
      "command": "local sync",
      "executable": "piggity",
      "result_identity": "piggity-cli-result/local/sync/v1",
      "source": {
        "line": 1101,
        "module": "piggity.local",
        "path": "reference/riverhog/applications/piggity/src/piggity/local.py",
        "symbol": "sync"
      }
    },
    {
      "command": "local repair",
      "executable": "piggity",
      "result_identity": "piggity-cli-result/local/repair/v1",
      "source": {
        "line": 1122,
        "module": "piggity.local",
        "path": "reference/riverhog/applications/piggity/src/piggity/local.py",
        "symbol": "repair"
      }
    }
  ],
  "cli_commands": [
    "local repair",
    "local sync"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.list_retrieval_plan_files",
      "source": {
        "line": 841,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.list_retrieval_plan_files"
      }
    }
  ],
  "method": "GET",
  "operation_id": "list_retrieval_plan_files",
  "path": "/v1/retrieval-plans/{plan_id}/files",
  "provider_evidence": null,
  "read_collection": {
    "authority": "retrieval-plan-files",
    "cursor_parameter": "start_ordinal",
    "kind": "exact-authority-page",
    "limit_parameter": "page_size"
  },
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1retrieval-plans~1{plan_id}~1files/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 30ff87fc6f5061dfb011295d2a61be5e3438af1a1a740de1bdb922b4736c6065 -->

```json
{
  "operationId": "list_retrieval_plan_files",
  "parameters": [
    {
      "in": "path",
      "name": "plan_id",
      "required": true,
      "schema": {
        "title": "Plan Id",
        "type": "string"
      }
    },
    {
      "in": "query",
      "name": "start_ordinal",
      "required": false,
      "schema": {
        "default": 0,
        "maximum": 10000,
        "minimum": 0,
        "title": "Start Ordinal",
        "type": "integer"
      }
    },
    {
      "in": "query",
      "name": "page_size",
      "required": false,
      "schema": {
        "default": 100,
        "maximum": 100,
        "minimum": 1,
        "title": "Page Size",
        "type": "integer"
      }
    },
    {
      "in": "header",
      "name": "If-Match",
      "required": true,
      "schema": {
        "pattern": "^\"[0-9a-f]{64}\"$",
        "title": "If-Match",
        "type": "string"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/RetrievalPlanFilePageOut"
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
        "invalid_state"
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
  "summary": "List Retrieval Plan Files",
  "tags": [
    "retrieval"
  ],
  "x-riverhog-interface": "client-only-primitive",
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "retrieval:manage"
      ]
    }
  ],
  "x-riverhog-read-collection": {
    "authority": "retrieval-plan-files",
    "cursor_parameter": "start_ordinal",
    "kind": "exact-authority-page",
    "limit_parameter": "page_size"
  }
}
```

</details>
