# GET /v1/retrieval-plans/{plan_id}/files

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:get-v1-retrieval-plans-plan-id-files:3aa9f6bf8b -->

List Retrieval Plan Files

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [retrieval-plans](families/retrieval-plans/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-02b9ee2a20"></a>
- <a id="s-f5a7d6ee6a"></a>`operationId`: list_retrieval_plan_files
- <a id="s-864d6dee41"></a>`summary`: List Retrieval Plan Files
- <a id="s-80e7646656"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-f9f5dd3ea1"></a>`plan_id` | path | yes | type="string" |
| <a id="s-b7b9300a2d"></a>`start_ordinal` | query | no | type="integer"; minimum=0; maximum=10000 |
| <a id="s-ee956f2b24"></a>`page_size` | query | no | type="integer"; minimum=1; maximum=100 |
| <a id="s-44a185df9a"></a>`If-Match` | header | yes | type="string"; pattern="^\"[0-9a-f]{64}\"$" |

### Responses

| Status | Description |
|---|---|
| <a id="s-85f358c0bc"></a>`200` | Successful Response |
| <a id="s-7badd60fa1"></a>`400` | Bad Request |
| <a id="s-9015f8bd0a"></a>`401` | Unauthorized |
| <a id="s-06ec61c8b6"></a>`403` | Forbidden |
| <a id="s-99f3ea6246"></a>`404` | Not Found |
| <a id="s-1ef560b954"></a>`409` | Conflict |
| <a id="s-df924e03b9"></a>`412` | Precondition Failed |
| <a id="s-533383f4a4"></a>`500` | Internal Server Error |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"authority":"retrieval-plan-files","cursor_parameter":"start_ordinal","kind":"exact-authority-page","limit_parameter":"page_size"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/retrieval-plans/{plan_id}/files](#s-02b9ee2a20) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-a7a670c975"></a>[parameter page_size](#s-ee956f2b24) | `value · schema-value · contract_max` | maximum=100; minimum=1 |
| <a id="s-3cb9b8fbc9"></a>[parameter start_ordinal](#s-b7b9300a2d) | `value · schema-value · contract_max` | maximum=10000; minimum=0 |

## Maintained corroboration

### Related interface records

- [Operation parity: list_retrieval_plan_files](../operation/operation-parity-list-retrieval-plan-files.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: RetrievalPlanFilePageOut](schemas-retrievalplanfilepageout.md)

## Governing policies

- <a id="pa-615b70a07b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-8842142a5a"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-6b15598369"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1retrieval-plans~1{plan_id}~1files/get`

### Exact owned JSON

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
