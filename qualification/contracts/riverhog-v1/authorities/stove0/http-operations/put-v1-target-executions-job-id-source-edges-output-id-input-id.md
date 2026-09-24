# PUT /v1/target-executions/{job_id}/source-edges/{output_id}/{input_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:stove0:put-v1-target-executions-job-id-source-ed-4cec468e6f:f0390888ef -->

Declare Target Execution Source Edge

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-aab8a01cb0"></a>
- <a id="s-b46c2b115c"></a>`operationId`: `"declare_target_execution_source_edge"`
- <a id="s-3e190d97bb"></a>`summary`: `"Declare Target Execution Source Edge"`
- <a id="s-e6314ae769"></a>`tags`: `["target-executions"]`
- <a id="s-d012cc6016"></a>`x-riverhog-interface`: `"client-only-primitive"`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-50bb08bc34"></a>`job_id` | path | yes | not declared | type="string"; title="Job Id" |
| <a id="s-2b509a1ef3"></a>`output_id` | path | yes | not declared | type="string"; title="Output Id" |
| <a id="s-eb4dbcaa10"></a>`input_id` | path | yes | not declared | type="string"; title="Input Id" |

### <a id="s-210a565500"></a>Request body

- `required`: `true`

| Media type | Schema |
|---|---|
| application/json | [OutputSourceEdge](../http-schemas/schemas-outputsourceedge.md) |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-5bcc8cce46"></a>`200` | Successful Response | application/json | [TargetCallbackAcknowledgement](../http-schemas/schemas-targetcallbackacknowledgement.md) | not declared |
| <a id="s-019edde68f"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-ab9e47f964"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-eb5b519e36"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-fd7a8a0f4c"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [stove0_target_client.TargetCallbackClient.declare_target_execution_source_edge](../../stove0-target-client/python/stove0-target-client-targetcallbackclient-declare-target-execution-source-edge.md)

### Referenced contract elements

- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)
- [schemas: OutputSourceEdge](../http-schemas/schemas-outputsourceedge.md)
- [schemas: TargetCallbackAcknowledgement](../http-schemas/schemas-targetcallbackacknowledgement.md)

## Governing policies

- <a id="pa-f5aa5215a7"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [some-implementations/stove0/application/server/src/stove0\_api/app.py::create\_app.&lt;locals&gt;.declare\_target\_execution\_source\_edge](../../../../../../some-implementations/stove0/application/server/src/stove0_api/app.py#L432)

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
      "public_identity": "stove0_target_client.TargetCallbackClient.declare_target_execution_source_edge",
      "source": {
        "line": 255,
        "module": "stove0_target_client.client",
        "path": "some-implementations/stove0/packages/target-client/src/stove0_target_client/client.py",
        "symbol": "TargetCallbackClient.declare_target_execution_source_edge"
      }
    }
  ],
  "method": "PUT",
  "operation_id": "declare_target_execution_source_edge",
  "path": "/v1/target-executions/{job_id}/source-edges/{output_id}/{input_id}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1target-executions~1{job_id}~1source-edges~1{output_id}~1{input_id}/put`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fe82d815e1a7044561a57a462dbe809c6ff42dd677938f25620381326eb5835c -->

```json
{
  "operationId": "declare_target_execution_source_edge",
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
      "name": "output_id",
      "required": true,
      "schema": {
        "title": "Output Id",
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
          "$ref": "#/components/schemas/OutputSourceEdge"
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
            "$ref": "#/components/schemas/ErrorOut"
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
            "$ref": "#/components/schemas/ErrorOut"
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
            "$ref": "#/components/schemas/ErrorOut"
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
            "$ref": "#/components/schemas/ErrorOut"
          }
        }
      },
      "description": "Internal Server Error",
      "x-riverhog-error-codes": [
        "internal_error"
      ]
    }
  },
  "summary": "Declare Target Execution Source Edge",
  "tags": [
    "target-executions"
  ],
  "x-riverhog-interface": "client-only-primitive"
}
```

</details>
