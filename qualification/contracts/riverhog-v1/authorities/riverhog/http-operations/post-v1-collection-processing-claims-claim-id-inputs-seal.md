# POST /v1/collection-processing-claims/{claim_id}/inputs/seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:post-v1-collection-processing-claims-clai-f28ae97d90:aa58357f65 -->

Seal Processing Claim Inputs

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-16b02ae3eb"></a>
- <a id="s-0e67b5e3a7"></a>`operationId`: seal_processing_claim_inputs
- <a id="s-22bcb81034"></a>`summary`: Seal Processing Claim Inputs
- <a id="s-bce74c0a32"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-3a45e4c1f6"></a>`claim_id` | path | yes | type="string"; pattern="^[0-9a-f]{64}$" |

### <a id="s-aeea5c9b93"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/ProcessingClaimFenceDocument"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-f765ce6d37"></a>`200` | Successful Response |
| <a id="s-20cbe87d39"></a>`400` | Bad Request |
| <a id="s-dbbbd9143e"></a>`401` | Unauthorized |
| <a id="s-667d1da24f"></a>`403` | Forbidden |
| <a id="s-cfdf254c03"></a>`500` | Internal Server Error |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-98f0ff82de"></a>[parameter claim_id](#s-3a45e4c1f6) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: ProcessingClaimFenceDocument](../http-schemas/schemas-processingclaimfencedocument.md)
- [schemas: ReceivingSetDocument](../http-schemas/schemas-receivingsetdocument.md)

## Governing policies

- <a id="pa-89bb60f690"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-ecfe2072fd"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

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
  "cli_commands": [],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "seal_processing_claim_inputs",
  "path": "/v1/collection-processing-claims/{claim_id}/inputs/seal",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-processing-claims~1{claim_id}~1inputs~1seal/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 271f8bd738f84cd8663e0bab72678ba07cc373a632b707a5cc4594ff2321a1e6 -->

```json
{
  "operationId": "seal_processing_claim_inputs",
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
          "$ref": "#/components/schemas/ProcessingClaimFenceDocument"
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
            "$ref": "#/components/schemas/ReceivingSetDocument"
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
  "summary": "Seal Processing Claim Inputs",
  "tags": [
    "collection-workflows"
  ],
  "x-riverhog-interface": "client-only-primitive",
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "collection-transforms:control"
      ]
    }
  ]
}
```
