# GET /v1/admissions/{admission_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:stove0:get-v1-admissions-admission-id:a4dbc11662 -->

Get Admission

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Operations](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1006ba465b"></a>
- <a id="s-6d67122943"></a>`operationId`: get_admission
- <a id="s-ec5d6eccd8"></a>`summary`: Get Admission

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-7c12b87afb"></a>`admission_id` | path | yes | type="string" |

### Responses

| Status | Description |
|---|---|
| <a id="s-b0cd202f5e"></a>`200` | Successful Response |
| <a id="s-01f2cf0d10"></a>`400` | Bad Request |
| <a id="s-a3aac7f8d3"></a>`401` | Unauthorized |
| <a id="s-ba083c67b5"></a>`403` | Forbidden |
| <a id="s-0775eed8e2"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [stove0-client admission show](../../stove0-client/cli/stove0-client-admission-show.md)

### Referenced contract dossiers

- [schemas: AdmissionView](../http-schemas/schemas-admissionview.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-efc4286ee1"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Operation qualification evidence

This evidence proves maintained client, CLI, response-authority, and provider qualification without creating a second semantic operation.

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "admission show"
  ],
  "client": "Stove0ApiClient",
  "method": "GET",
  "operation_id": "get_admission",
  "path": "/v1/admissions/{admission_id}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1admissions~1{admission_id}/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 12041e13ac24c0fcc1163bdc107830c78273611f2dde529441a3312707a8105b -->

```json
{
  "operationId": "get_admission",
  "parameters": [
    {
      "in": "path",
      "name": "admission_id",
      "required": true,
      "schema": {
        "title": "Admission Id",
        "type": "string"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/AdmissionView"
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
  "summary": "Get Admission",
  "tags": [
    "admissions"
  ]
}
```
