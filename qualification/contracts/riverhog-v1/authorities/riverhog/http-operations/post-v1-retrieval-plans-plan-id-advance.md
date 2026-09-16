# POST /v1/retrieval-plans/{plan_id}/advance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:post-v1-retrieval-plans-plan-id-advance:89fe188d31 -->

Advance Retrieval Plan

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-eee1d15d4d"></a>
- <a id="s-3905b4ab13"></a>`operationId`: advance_retrieval_plan
- <a id="s-81ae26728e"></a>`summary`: Advance Retrieval Plan
- <a id="s-945eb7424f"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-99944139c0"></a>`plan_id` | path | yes | type="string" |

### Responses

| Status | Description |
|---|---|
| <a id="s-3fb020b34e"></a>`200` | Successful Response |
| <a id="s-a150a2ac0e"></a>`400` | Bad Request |
| <a id="s-95748db20a"></a>`401` | Unauthorized |
| <a id="s-960c586011"></a>`403` | Forbidden |
| <a id="s-0ed376610d"></a>`404` | Not Found |
| <a id="s-673acbfb18"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: RetrievalPlanOut](../http-schemas/schemas-retrievalplanout.md)

## Governing policies

- <a id="pa-7b78f8f089"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [
    "local repair",
    "local sync"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "advance_retrieval_plan",
  "path": "/v1/retrieval-plans/{plan_id}/advance",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1retrieval-plans~1{plan_id}~1advance/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6c0bd90f12039ce3259702c25022ec803151a6457dc24652bded7a8c05cbaf45 -->

```json
{
  "operationId": "advance_retrieval_plan",
  "parameters": [
    {
      "in": "path",
      "name": "plan_id",
      "required": true,
      "schema": {
        "title": "Plan Id",
        "type": "string"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/RetrievalPlanOut"
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
  "summary": "Advance Retrieval Plan",
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
  ]
}
```
