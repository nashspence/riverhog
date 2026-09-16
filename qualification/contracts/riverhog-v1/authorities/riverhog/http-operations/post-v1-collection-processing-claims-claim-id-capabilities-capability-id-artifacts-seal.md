# POST /v1/collection-processing-claims/{claim_id}/capabilities/{capability_id}/artifacts/seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:post-v1-collection-processing-claims-clai-1adf6d61a9:1e7eebb7ed -->

Seal Transform Capability Artifacts

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-96e6657254"></a>
- <a id="s-7d42e7a5fe"></a>`operationId`: seal_transform_capability_artifacts
- <a id="s-495f2933da"></a>`summary`: Seal Transform Capability Artifacts
- <a id="s-5e482a2c54"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-a77e4086ba"></a>`claim_id` | path | yes | type="string"; pattern="^[0-9a-f]{64}$" |
| <a id="s-5564f94db3"></a>`capability_id` | path | yes | type="string" |

### <a id="s-740e76aa18"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/ProcessingClaimFenceDocument"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-52c6a2aa5e"></a>`200` | Successful Response |
| <a id="s-5e78616014"></a>`400` | Bad Request |
| <a id="s-c16d957b86"></a>`401` | Unauthorized |
| <a id="s-148fc325be"></a>`403` | Forbidden |
| <a id="s-3c05b5acb7"></a>`500` | Internal Server Error |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-f5b80bff84"></a>[parameter claim_id](#s-a77e4086ba) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactReceivingSetDocument](../http-schemas/schemas-artifactreceivingsetdocument.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: ProcessingClaimFenceDocument](../http-schemas/schemas-processingclaimfencedocument.md)

## Governing policies

- <a id="pa-6a933b59fa"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-5c55d6a3db"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

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
  "operation_id": "seal_transform_capability_artifacts",
  "path": "/v1/collection-processing-claims/{claim_id}/capabilities/{capability_id}/artifacts/seal",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-processing-claims~1{claim_id}~1capabilities~1{capability_id}~1artifacts~1seal/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6b1216049fe2720b9db7c1bf412923b7bb91d9b0bdefc24993f458372b9149b5 -->

```json
{
  "operationId": "seal_transform_capability_artifacts",
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
    },
    {
      "in": "path",
      "name": "capability_id",
      "required": true,
      "schema": {
        "title": "Capability Id",
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
            "$ref": "#/components/schemas/ArtifactReceivingSetDocument"
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
  "summary": "Seal Transform Capability Artifacts",
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
