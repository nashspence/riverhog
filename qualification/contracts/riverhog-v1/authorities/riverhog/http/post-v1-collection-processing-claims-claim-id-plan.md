# POST /v1/collection-processing-claims/{claim_id}/plan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:post-v1-collection-processing-claims-claim-id-plan:344520d27b -->

Seal Processing Claim Plan

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-e6a3728b60"></a>
- <a id="s-56915425c5"></a>`operationId`: seal_processing_claim_plan
- <a id="s-15527c9a9f"></a>`summary`: Seal Processing Claim Plan
- <a id="s-a8d1862d88"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-9ce92e42b8"></a>`claim_id` | path | yes | type="string"; pattern="^[0-9a-f]{64}$" |

### <a id="s-1988f9a85f"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/ProcessingClaimPlanSealDocument"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-1502dfae14"></a>`200` | Successful Response |
| <a id="s-f1c635f0bc"></a>`400` | Bad Request |
| <a id="s-cfdaad2fc1"></a>`401` | Unauthorized |
| <a id="s-d5113150d1"></a>`403` | Forbidden |
| <a id="s-75c31e6d79"></a>`404` | Not Found |
| <a id="s-341237689d"></a>`409` | Conflict |
| <a id="s-5049136fa0"></a>`500` | Internal Server Error |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-288d323239"></a>[parameter claim_id](#s-9ce92e42b8) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [Operation parity: seal_processing_claim_plan](../operation/operation-parity-seal-processing-claim-plan.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: ProcessingClaimDocument](schemas-processingclaimdocument.md)
- [schemas: ProcessingClaimPlanSealDocument](schemas-processingclaimplansealdocument.md)

## Governing policies

- <a id="pa-d6cf4df40c"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-16f30f98e3"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-processing-claims~1{claim_id}~1plan/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ea0f3a4e74d72badfb51511fadf4c1f1836cf574f012eecf48044750d2efcf0d -->

```json
{
  "operationId": "seal_processing_claim_plan",
  "parameters": [
    {
      "in": "path",
      "name": "claim_id",
      "required": true,
      "schema": {
        "pattern": "^[0-9a-f]{64}$",
        "title": "Claim Id",
        "type": "string"
      }
    }
  ],
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/ProcessingClaimPlanSealDocument"
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
            "$ref": "#/components/schemas/ProcessingClaimDocument"
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
        "conflict",
        "invalid_state"
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
  "summary": "Seal Processing Claim Plan",
  "tags": [
    "collection-workflows"
  ],
  "x-riverhog-interface": "client-only-primitive",
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "collection-transforms:execute"
      ]
    }
  ]
}
```
