# GET /v1/collections/{collection_id}/provenance/verification

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-collections-collection-id-provenan-7bd7f79e98:bb9ad115b6 -->

Get Collection Provenance Verification

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-04bd0939af"></a>
- <a id="s-21b70cfd23"></a>`operationId`: get_collection_provenance_verification
- <a id="s-d23b62a638"></a>`summary`: Get Collection Provenance Verification
- <a id="s-b0bf2060df"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-fe54297ab2"></a>`collection_id` | path | yes | type="integer"; minimum=1 |

### Responses

| Status | Description |
|---|---|
| <a id="s-d1f90bc6b1"></a>`200` | Successful Response |
| <a id="s-4d7e0340dc"></a>`400` | Bad Request |
| <a id="s-dd22622f62"></a>`401` | Unauthorized |
| <a id="s-06f1a4742b"></a>`403` | Forbidden |
| <a id="s-eec8024348"></a>`404` | Not Found |
| <a id="s-de46d6f4a9"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [piggity collection provenance verification-show](../../piggity/cli/piggity-collection-provenance-verification-show.md)
- [piggity collection provenance verify](../../piggity/cli/piggity-collection-provenance-verify.md)

### Referenced contract dossiers

- [schemas: CollectionProvenanceVerificationJobOut](../http-schemas/schemas-collectionprovenanceverificationjobout.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-e42492a0a5"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

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
  "classification": "human-cli+json",
  "cli_commands": [
    "collection provenance verification-show",
    "collection provenance verify"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "get_collection_provenance_verification",
  "path": "/v1/collections/{collection_id}/provenance/verification",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collections~1{collection_id}~1provenance~1verification/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a06799ad2ad530afa9f473f7288f959dde3daca0b6b52eb255312ea350fd0412 -->

```json
{
  "operationId": "get_collection_provenance_verification",
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
  "summary": "Get Collection Provenance Verification",
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
