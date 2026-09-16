# PUT /v1/collection-processing-claims/{claim_id}/capabilities/{capability_id}/artifacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:put-v1-collection-processing-claims-claim-35a561aa3f:87092575bd -->

Append Transform Capability Artifacts

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-7fa2a829fd"></a>
- <a id="s-fef5d56060"></a>`operationId`: append_transform_capability_artifacts
- <a id="s-3461877a59"></a>`summary`: Append Transform Capability Artifacts
- <a id="s-3d32ce3f94"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-add0ae484d"></a>`claim_id` | path | yes | type="string"; pattern="^[0-9a-f]{64}$" |
| <a id="s-6e25de8d57"></a>`capability_id` | path | yes | type="string" |

### <a id="s-ba75abb644"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/CollectionArtifactBatchDocument"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-d951f977c5"></a>`200` | Successful Response |
| <a id="s-0fe028624c"></a>`400` | Bad Request |
| <a id="s-c44bdfcd28"></a>`401` | Unauthorized |
| <a id="s-73416dd8f6"></a>`403` | Forbidden |
| <a id="s-e52a0180cd"></a>`500` | Internal Server Error |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-0bbeca14e9"></a>[parameter claim_id](#s-add0ae484d) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactReceivingSetDocument](../http-schemas/schemas-artifactreceivingsetdocument.md)
- [schemas: CollectionArtifactBatchDocument](../http-schemas/schemas-collectionartifactbatchdocument.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-d7fee2536b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-1d95f2e52e"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

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
  "method": "PUT",
  "operation_id": "append_transform_capability_artifacts",
  "path": "/v1/collection-processing-claims/{claim_id}/capabilities/{capability_id}/artifacts",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-processing-claims~1{claim_id}~1capabilities~1{capability_id}~1artifacts/put`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 919d806868b16b4c4ad965ee965a67a935d6d629dd371bb93af95c7cde77fe6c -->

```json
{
  "operationId": "append_transform_capability_artifacts",
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
          "$ref": "#/components/schemas/CollectionArtifactBatchDocument"
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
  "summary": "Append Transform Capability Artifacts",
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
