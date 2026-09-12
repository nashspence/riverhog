# GET /v1/admission-policies

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:get-v1-admission-policies:5153661f2e -->

List Admission Policies

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [admission-policies](families/admission-policies/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-9cdc3681b17f"></a>
- <a id="s-d210f67bfc07"></a>`operationId`: list_admission_policies
- <a id="s-0487d2dfd0ac"></a>`summary`: List Admission Policies

### Responses

| Status | Description |
|---|---|
| <a id="s-7e95b1072855"></a>`200` | Successful Response |
| <a id="s-4f37d64ec94b"></a>`400` | Bad Request |
| <a id="s-313cc0359fd7"></a>`401` | Unauthorized |
| <a id="s-5b7c3932b3e0"></a>`403` | Forbidden |
| <a id="s-1a01fac0f07a"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: list_admission_policies](../operation/operation-parity-list-admission-policies.md)

### Referenced contract dossiers

- [schemas: AdmissionPolicyCatalogView](schemas-admissionpolicycatalogview.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)

## Governing policies

- <a id="pa-b41293d1bba2"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1admission-policies/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ba572c1069bce623198498017f3525eaf02bfbbb64e373211c3048be888993ae -->

```json
{
  "operationId": "list_admission_policies",
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/AdmissionPolicyCatalogView"
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
  "summary": "List Admission Policies",
  "tags": [
    "admissions"
  ]
}
```
