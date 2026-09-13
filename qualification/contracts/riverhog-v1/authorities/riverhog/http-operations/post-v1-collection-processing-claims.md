# POST /v1/collection-processing-claims

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:post-v1-collection-processing-claims:94feba3584 -->

Create Or Resume Processing Claim

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-1d216e45e1"></a>
- <a id="s-0415bfc8bd"></a>`operationId`: create_or_resume_processing_claim
- <a id="s-dbd084c980"></a>`summary`: Create Or Resume Processing Claim
- <a id="s-6808f3b568"></a>`security`: `[{"HTTPBearer": []}]`

### <a id="s-9a28721f3d"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/ProcessingClaimCreateDocument"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-1a7f39cecc"></a>`200` | Successful Response |
| <a id="s-2702385e59"></a>`400` | Bad Request |
| <a id="s-4407627fc0"></a>`401` | Unauthorized |
| <a id="s-76adc32432"></a>`403` | Forbidden |
| <a id="s-db68694f14"></a>`404` | Not Found |
| <a id="s-c03e23d464"></a>`409` | Conflict |
| <a id="s-0591f37fc9"></a>`500` | Internal Server Error |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: ProcessingClaimCreateDocument](../http-schemas/schemas-processingclaimcreatedocument.md)
- [schemas: ProcessingClaimDocument](../http-schemas/schemas-processingclaimdocument.md)

## Governing policies

- <a id="pa-9e40e36168"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Operation qualification evidence

This evidence proves maintained client, CLI, response-authority, and provider qualification without creating a second semantic operation.

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "create_or_resume_processing_claim",
  "path": "/v1/collection-processing-claims",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-processing-claims/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1d44f4fc43b3573039d01d412f299182a0f428e553ead1e68e4cf37da9b0fbfe -->

```json
{
  "operationId": "create_or_resume_processing_claim",
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/ProcessingClaimCreateDocument"
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
  "summary": "Create Or Resume Processing Claim",
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
