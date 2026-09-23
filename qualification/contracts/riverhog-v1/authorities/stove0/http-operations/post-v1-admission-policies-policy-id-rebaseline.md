# POST /v1/admission-policies/{policy_id}:rebaseline

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:stove0:post-v1-admission-policies-policy-id-rebaseline:7df07b33cc -->

Rebaseline Admission Policy

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-97afae1f0a"></a>
- <a id="s-2fb148ddb5"></a>`operationId`: `"rebaseline_admission_policy"`
- <a id="s-daf751e989"></a>`summary`: `"Rebaseline Admission Policy"`
- <a id="s-8d61fa1e69"></a>`tags`: `["admissions"]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-b3113ce543"></a>`policy_id` | path | yes | not declared | type="string"; title="Policy Id" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-5c4133d977"></a>`200` | Successful Response | application/json | [AdmissionPolicyStatus](../http-schemas/schemas-admissionpolicystatus.md) | not declared |
| <a id="s-1a395b5f4d"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-fda7a3dba0"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-971a0e621b"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-c3f529e56c"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [stove0 admission policy rebaseline](../../a-stove0-cli/cli/stove0-admission-policy-rebaseline.md)
- [stove0_api_client.Stove0ApiClient.rebaseline_admission_policy](../../stove0-api-client/python/stove0-api-client-stove0apiclient-rebaseline-admission-policy.md)

### Referenced contract elements

- [schemas: AdmissionPolicyStatus](../http-schemas/schemas-admissionpolicystatus.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-c6202a3bd8"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [some-implementations/stove0/application/server/src/stove0\_api/app.py::create\_app.&lt;locals&gt;.rebaseline\_admission\_policy](../../../../../../some-implementations/stove0/application/server/src/stove0_api/app.py#L583)

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

<details>
<summary>Exact structural binding record</summary>

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_bindings": [
    {
      "command": "admission policy rebaseline",
      "executable": "stove0",
      "result_identity": "stove0-cli-result/admission/policy/rebaseline/v1",
      "source": {
        "line": 195,
        "module": "a_stove0_cli.main",
        "path": "some-implementations/stove0/application/client/src/a_stove0_cli/main.py",
        "symbol": "rebaseline_admission_policy"
      }
    }
  ],
  "cli_commands": [
    "admission policy rebaseline"
  ],
  "client": "Stove0ApiClient",
  "client_bindings": [
    {
      "public_identity": "stove0_api_client.Stove0ApiClient.rebaseline_admission_policy",
      "source": {
        "line": 164,
        "module": "stove0_api_client.client",
        "path": "some-implementations/stove0/packages/api-client/src/stove0_api_client/client.py",
        "symbol": "Stove0ApiClient.rebaseline_admission_policy"
      }
    }
  ],
  "method": "POST",
  "operation_id": "rebaseline_admission_policy",
  "path": "/v1/admission-policies/{policy_id}:rebaseline",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1admission-policies~1{policy_id}:rebaseline/post`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
