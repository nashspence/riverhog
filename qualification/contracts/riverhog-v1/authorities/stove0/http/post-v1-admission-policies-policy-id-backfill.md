# POST /v1/admission-policies/{policy_id}:backfill

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:post-v1-admission-policies-policy-id-backfill:bcad9123a8 -->

Backfill Admission Policy

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [admission-policies](families/admission-policies/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-a03a786413"></a>
- <a id="s-4b5fd3fd50"></a>`operationId`: backfill_admission_policy
- <a id="s-51b7a7f933"></a>`summary`: Backfill Admission Policy

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-b6cc543640"></a>`policy_id` | path | yes | type="string" |

### Responses

| Status | Description |
|---|---|
| <a id="s-c589a46b03"></a>`200` | Successful Response |
| <a id="s-33d86e5074"></a>`400` | Bad Request |
| <a id="s-96370e1b48"></a>`401` | Unauthorized |
| <a id="s-15c08c80dd"></a>`403` | Forbidden |
| <a id="s-06c122ae8e"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: backfill_admission_policy](../operation/operation-parity-backfill-admission-policy.md)

### Referenced contract dossiers

- [schemas: AdmissionPolicyStatus](schemas-admissionpolicystatus.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)

## Governing policies

- <a id="pa-4d3f809d2f"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1admission-policies~1{policy_id}:backfill/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c6a889883bb0071e7334b8c5a583f17d6c7e06d2e0d33e3911abcceb104fe3ef -->

```json
{
  "operationId": "backfill_admission_policy",
  "parameters": [
    {
      "in": "path",
      "name": "policy_id",
      "required": true,
      "schema": {
        "title": "Policy Id",
        "type": "string"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/AdmissionPolicyStatus"
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
  "summary": "Backfill Admission Policy",
  "tags": [
    "admissions"
  ]
}
```
