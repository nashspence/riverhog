# POST /v1/admission-policies/{policy_id}:rebaseline

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:stove0:post-v1-admission-policies-policy-id-rebaseline:7df07b33cc -->

Rebaseline Admission Policy

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Operations](index.md) |
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

- [stove0-client admission policy rebaseline](../../stove0-client/cli/stove0-client-admission-policy-rebaseline.md)

### Referenced contract dossiers

- [schemas: AdmissionPolicyStatus](../http-schemas/schemas-admissionpolicystatus.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-c6202a3bd8"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

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
    "admission policy rebaseline"
  ],
  "client": "Stove0ApiClient",
  "method": "POST",
  "operation_id": "rebaseline_admission_policy",
  "path": "/v1/admission-policies/{policy_id}:rebaseline",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```

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
