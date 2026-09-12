# POST /v1/admission-policies/{policy_id}:rebaseline

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:post-v1-admission-policies-policy-id-rebaseline:174f746dd2 -->

Rebaseline Admission Policy

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [admission-policies](families/admission-policies/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-97afae1f0a"></a>
- <a id="s-2fb148ddb5"></a>`operationId`: rebaseline_admission_policy
- <a id="s-daf751e989"></a>`summary`: Rebaseline Admission Policy

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-b3113ce543"></a>`policy_id` | path | yes | type="string" |

### Responses

| Status | Description |
|---|---|
| <a id="s-5c4133d977"></a>`200` | Successful Response |
| <a id="s-1a395b5f4d"></a>`400` | Bad Request |
| <a id="s-fda7a3dba0"></a>`401` | Unauthorized |
| <a id="s-971a0e621b"></a>`403` | Forbidden |
| <a id="s-c3f529e56c"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: rebaseline_admission_policy](../operation/operation-parity-rebaseline-admission-policy.md)

### Referenced contract dossiers

- [schemas: AdmissionPolicyStatus](schemas-admissionpolicystatus.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)

## Governing policies

- <a id="pa-b591947acb"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1admission-policies~1{policy_id}:rebaseline/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 438b32a39bf99f0d50510eadbe88270a08edeacc923a7cc64072a142ee402897 -->

```json
{
  "operationId": "rebaseline_admission_policy",
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
  "summary": "Rebaseline Admission Policy",
  "tags": [
    "admissions"
  ]
}
```
