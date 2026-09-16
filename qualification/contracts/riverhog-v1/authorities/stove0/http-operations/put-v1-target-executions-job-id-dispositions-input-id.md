# PUT /v1/target-executions/{job_id}/dispositions/{input_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:stove0:put-v1-target-executions-job-id-dispositi-608495d368:84e3f24844 -->

Declare Target Execution Disposition

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-cd0c980798"></a>
- <a id="s-dcc45a72b8"></a>`operationId`: declare_target_execution_disposition
- <a id="s-b325b56157"></a>`summary`: Declare Target Execution Disposition

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-3770379334"></a>`job_id` | path | yes | not declared | type="string" |
| <a id="s-738b7756eb"></a>`input_id` | path | yes | not declared | type="string" |

### <a id="s-4af8a05da7"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/InputDispositionDeclaration"}}}, "required": true}`

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-a5ce55528a"></a>`200` | Successful Response | application/json | [TargetCallbackAcknowledgement](../http-schemas/schemas-targetcallbackacknowledgement.md) | not declared |
| <a id="s-2baee2a5b5"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-5f90d24b53"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-7737fe8871"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-ccb3dc68bd"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [stove0_target_client.TargetCallbackClient.declare_target_execution_disposition](../../stove0-target-client/python/stove0-target-client-targetcallbackclient-declare-target-execution-disposition.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: InputDispositionDeclaration](../http-schemas/schemas-inputdispositiondeclaration.md)
- [schemas: TargetCallbackAcknowledgement](../http-schemas/schemas-targetcallbackacknowledgement.md)

## Governing policies

- <a id="pa-8aae99dafc"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **OpenAPI authority:** [openapi:stove0](../../../evidence/sources.md#src-52e6e32124)
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`
- **Handler:** [reference/stove0/application/server/src/stove0_api/app.py::create_app.<locals>.declare_target_execution_disposition](../../../../../../reference/stove0/application/server/src/stove0_api/app.py#L410)

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

<details>
<summary>Exact structural binding record</summary>

```json
{
  "application": "stove0",
  "classification": "client-only-primitive",
  "cli_bindings": [],
  "cli_commands": [],
  "client": "TargetCallbackClient",
  "client_bindings": [
    {
      "public_identity": "stove0_target_client.TargetCallbackClient.declare_target_execution_disposition",
      "source": {
        "line": 243,
        "module": "stove0_target_client.client",
        "path": "reference/stove0/packages/target-client/src/stove0_target_client/client.py",
        "symbol": "TargetCallbackClient.declare_target_execution_disposition"
      }
    }
  ],
  "method": "PUT",
  "operation_id": "declare_target_execution_disposition",
  "path": "/v1/target-executions/{job_id}/dispositions/{input_id}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1target-executions~1{job_id}~1dispositions~1{input_id}/put`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fd8022168ec6cf2ef70de7af26319d173e589ad80262dc4e8b61c2044ae714f7 -->

```json
{
  "operationId": "declare_target_execution_disposition",
  "parameters": [
    {
      "in": "path",
      "name": "job_id",
      "required": true,
      "schema": {
        "title": "Job Id",
        "type": "string"
      }
    },
    {
      "in": "path",
      "name": "input_id",
      "required": true,
      "schema": {
        "title": "Input Id",
        "type": "string"
      }
    }
  ],
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/InputDispositionDeclaration"
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
            "$ref": "#/components/schemas/TargetCallbackAcknowledgement"
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
  "summary": "Declare Target Execution Disposition",
  "tags": [
    "target-executions"
  ],
  "x-riverhog-interface": "client-only-primitive"
}
```

</details>
