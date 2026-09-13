# POST /v1/collections/{collection_id}/provenance/verification

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:post-v1-collections-collection-id-provena-8ee667a419:851a47e434 -->

Request Collection Provenance Verification

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-86f73af968"></a>
- <a id="s-94fbc2db28"></a>`operationId`: request_collection_provenance_verification
- <a id="s-ef1c818a31"></a>`summary`: Request Collection Provenance Verification
- <a id="s-ffa16df624"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-28085f6159"></a>`collection_id` | path | yes | type="integer"; minimum=1 |

### Responses

| Status | Description |
|---|---|
| <a id="s-b2b98826d0"></a>`200` | Successful Response |
| <a id="s-8b632c5bb5"></a>`400` | Bad Request |
| <a id="s-ef4d7dfc78"></a>`401` | Unauthorized |
| <a id="s-163f344877"></a>`403` | Forbidden |
| <a id="s-50b7d59a4f"></a>`404` | Not Found |
| <a id="s-9ff961122a"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [piggity collection provenance verify](../../piggity/cli/piggity-collection-provenance-verify.md)

### Referenced contract dossiers

- [schemas: CollectionProvenanceVerificationJobOut](../http-schemas/schemas-collectionprovenanceverificationjobout.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-3b7a4ec14e"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

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
    "collection provenance verify"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "request_collection_provenance_verification",
  "path": "/v1/collections/{collection_id}/provenance/verification",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collections~1{collection_id}~1provenance~1verification/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f0ec010b277f84d3689b5206f18dab231b363fe7278235dd411e9131ea3d3e52 -->

```json
{
  "operationId": "request_collection_provenance_verification",
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
  "summary": "Request Collection Provenance Verification",
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
