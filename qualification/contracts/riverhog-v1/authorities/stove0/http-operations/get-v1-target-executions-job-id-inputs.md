# GET /v1/target-executions/{job_id}/inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:stove0:get-v1-target-executions-job-id-inputs:6c7f3ca62d -->

Get Target Execution Inputs

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-dd498ea95c"></a>
- <a id="s-0754069f02"></a>`operationId`: `"get_target_execution_inputs"`
- <a id="s-8899a415e1"></a>`summary`: `"Get Target Execution Inputs"`
- <a id="s-dba21f2f5d"></a>`tags`: `["target-executions"]`
- <a id="s-a22e55d9dc"></a>`x-riverhog-interface`: `"client-only-primitive"`
- <a id="s-490fc10b20"></a>`x-riverhog-read-collection`: `{"authority":"target-input-authority","cursor_parameter":"continuation","fixed_limit":256,"kind":"exact-authority-page"}`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-71bac0a8b3"></a>`job_id` | path | yes | not declared | type="string"; title="Job Id" |
| <a id="s-f3e43785ca"></a>`continuation` | query | no | not declared | anyOf=[(type="string"); (type="null")]; title="Continuation" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-9070ff8872"></a>`200` | Successful Response | application/json | [TargetInputPage](../http-schemas/schemas-targetinputpage.md) | not declared |
| <a id="s-06d3c2e4e6"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-7c20e23ee1"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-a3ab3c82cc"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-f3a5b7a840"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: progression={"authority":"target-input-authority","cursor_parameter":"continuation","fixed_limit":256,"kind":"exact-authority-page"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/target-executions/{job_id}/inputs](#s-dd498ea95c) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

### Evidence gaps

The named contract groups have recorded evidence gaps in the following guarantees. Each group's page identifies its exact open guarantees and candidate tests:

- Each step stays within its declared limits.
- Continuing the work makes progress toward its declared completion.
- The operation works across multiple pages or chunks.
- Required data or work is not silently left out.
- Work can resume after a restart as its contract requires.

These guarantees let large tasks proceed in smaller steps: a limit on one page or chunk must not become a hidden limit on the whole task. Returning a first page correctly does not establish that continuation or recovery works. Capacity may explicitly reject, defer, or throttle work; it must not silently omit work.

Existing tests may establish individual cases. The gaps retain their recorded group-wide scope and do not establish a bug in every linked contract. Completion follows each contract's rules; mutable browsing carries no implied snapshot guarantee.

Required by: [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb).

Exact evidence groups for this contract element:

- [stove0-read-collection-progression/v1](../../../evidence/qualifications/stove0-read-collection-progression-v1/index.md)

## Maintained corroboration

### Related interface records

- [stove0_target_client.TargetCallbackClient.get_target_execution_inputs](../../stove0-target-client/python/stove0-target-client-targetcallbackclient-get-target-execution-inputs.md)

### Referenced contract elements

- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)
- [schemas: TargetInputPage](../http-schemas/schemas-targetinputpage.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-23b18031cb"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-88288ccf0d"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [some-implementations/stove0/application/server/src/stove0\_api/app.py::create\_app.&lt;locals&gt;.get\_target\_execution\_inputs](../../../../../../some-implementations/stove0/application/server/src/stove0_api/app.py#L357)

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
      "public_identity": "stove0_target_client.TargetCallbackClient.get_target_execution_inputs",
      "source": {
        "line": 221,
        "module": "stove0_target_client.client",
        "path": "some-implementations/stove0/packages/target-client/src/stove0_target_client/client.py",
        "symbol": "TargetCallbackClient.get_target_execution_inputs"
      }
    }
  ],
  "method": "GET",
  "operation_id": "get_target_execution_inputs",
  "path": "/v1/target-executions/{job_id}/inputs",
  "provider_evidence": null,
  "read_collection": {
    "authority": "target-input-authority",
    "cursor_parameter": "continuation",
    "fixed_limit": 256,
    "kind": "exact-authority-page"
  },
  "response_authority": "canonical-document"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1target-executions~1{job_id}~1inputs/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9a864dbfca374974c703a2c09fd2e41629f164238c935614a19cef5ef15d08d0 -->

```json
{
  "operationId": "get_target_execution_inputs",
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
      "in": "query",
      "name": "continuation",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "type": "string"
          },
          {
            "type": "null"
          }
        ],
        "title": "Continuation"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/TargetInputPage"
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
  "summary": "Get Target Execution Inputs",
  "tags": [
    "target-executions"
  ],
  "x-riverhog-interface": "client-only-primitive",
  "x-riverhog-read-collection": {
    "authority": "target-input-authority",
    "cursor_parameter": "continuation",
    "fixed_limit": 256,
    "kind": "exact-authority-page"
  }
}
```

</details>
