# DELETE /v1/collections/{collection_id}/provenance/verification

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:delete-v1-collections-collection-id-prove-2f3a23a886:98e4f1b88e -->

Cancel Collection Provenance Verification

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-8b0234e95b"></a>
- <a id="s-2d83551345"></a>`operationId`: cancel_collection_provenance_verification
- <a id="s-e4a0e350f0"></a>`summary`: Cancel Collection Provenance Verification
- <a id="s-83b8c96050"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-a219e42480"></a>`collection_id` | path | yes | type="integer"; minimum=1 |

### Responses

| Status | Description |
|---|---|
| <a id="s-b9769da1e9"></a>`200` | Successful Response |
| <a id="s-c374871f77"></a>`400` | Bad Request |
| <a id="s-b631b89758"></a>`401` | Unauthorized |
| <a id="s-d5c44a1a29"></a>`403` | Forbidden |
| <a id="s-180fc2cae1"></a>`404` | Not Found |
| <a id="s-44a836a802"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [piggity collection provenance verification-cancel](../../piggity/cli/piggity-collection-provenance-verification-cancel.md)

### Referenced contract dossiers

- [schemas: CollectionProvenanceVerificationJobOut](../http-schemas/schemas-collectionprovenanceverificationjobout.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-ee77d7df4f"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

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
  "classification": "human-cli+json",
  "cli_commands": [
    "collection provenance verification-cancel"
  ],
  "client": "ApiClient",
  "method": "DELETE",
  "operation_id": "cancel_collection_provenance_verification",
  "path": "/v1/collections/{collection_id}/provenance/verification",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collections~1{collection_id}~1provenance~1verification/delete`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fd61b09ce4381f6170061d406817c4a075c0792b7c70578dc51e754f7ba27606 -->

```json
{
  "operationId": "cancel_collection_provenance_verification",
  "parameters": [
    {
      "in": "path",
      "name": "collection_id",
      "required": true,
      "schema": {
        "minimum": 1,
        "title": "Collection Id",
        "type": "integer"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/CollectionProvenanceVerificationJobOut"
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
  "summary": "Cancel Collection Provenance Verification",
  "tags": [
    "provenance"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "provenance:read"
      ]
    }
  ]
}
```
