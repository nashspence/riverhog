# POST /v1/run

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog-ftp-adapter:post-v1-run:7b40c3a77b -->

Run Pass

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-98a41252cc"></a>
- <a id="s-b09291ebea"></a>`operationId`: `"run_ftp_adapter_pass"`
- <a id="s-3f252e3855"></a>`security`: `[{"RiverhogFtpAdapterBearer":[]}]`
- <a id="s-a1173de3e1"></a>`summary`: `"Run Pass"`
- <a id="s-b91b0f4ab2"></a>`tags`: `["operations"]`

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-52ad3c0e19"></a>`200` | Successful Response | application/json | type="object"; additionalProperties=(any JSON value); title="Response Run Ftp Adapter Pass" | not declared |
| <a id="s-5adeebb411"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-d23416f258"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-2d6c5f3aff"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-3a833d8f38"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog-ftp-adapter"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-b44fcc10a7"></a>[response 200 · content · application/json](#s-52ad3c0e19) | `cardinality · entries · operational_policy` | shared above |

## Maintained corroboration

### Related interface records

- [riverhog-ftp-adapter run](../cli/riverhog-ftp-adapter-run.md)
- [riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient.run_ftp_adapter_pass](../../riverhog-ftp-adapter-api-client/python/riverhog-ftp-adapter-api-client-riverhogftpadapterclient-run-ftp-adapter-pass.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-b22c5f6cca"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-17df150351"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **OpenAPI authority:** [openapi:riverhog-ftp-adapter](../../../evidence/sources.md#src-c3a51ac29a)
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`
- **Handler:** [reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py::create_app.<locals>.run_pass](../../../../../../reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py#L283)

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

<details>
<summary>Exact structural binding record</summary>

```json
{
  "application": "riverhog-ftp-adapter",
  "classification": "human-cli+json",
  "cli_bindings": [
    {
      "command": "run",
      "source": {
        "line": 389,
        "module": "riverhog_ftp_adapter.app",
        "path": "reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/app.py",
        "symbol": "_run_command"
      }
    }
  ],
  "cli_commands": [
    "run"
  ],
  "client": "RiverhogFtpAdapterClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient.run_ftp_adapter_pass",
      "source": {
        "line": 99,
        "module": "riverhog_ftp_adapter_api_client.client",
        "path": "reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py",
        "symbol": "RiverhogFtpAdapterClient.run_ftp_adapter_pass"
      }
    }
  ],
  "method": "POST",
  "operation_id": "run_ftp_adapter_pass",
  "path": "/v1/run",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog-ftp-adapter/paths/~1v1~1run/post`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7e6f53229b371ad969314301be141f1e634e39bc0ce8e871f58e221a396ea819 -->

```json
{
  "operationId": "run_ftp_adapter_pass",
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "additionalProperties": true,
            "title": "Response Run Ftp Adapter Pass",
            "type": "object"
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
  "security": [
    {
      "RiverhogFtpAdapterBearer": []
    }
  ],
  "summary": "Run Pass",
  "tags": [
    "operations"
  ]
}
```

</details>
