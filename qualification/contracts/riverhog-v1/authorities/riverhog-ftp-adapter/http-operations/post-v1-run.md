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
- <a id="s-b09291ebea"></a>`operationId`: run_ftp_adapter_pass
- <a id="s-a1173de3e1"></a>`summary`: Run Pass
- <a id="s-3f252e3855"></a>`security`: `[{"RiverhogFtpAdapterBearer": []}]`

### Responses

| Status | Description |
|---|---|
| <a id="s-52ad3c0e19"></a>`200` | Successful Response |
| <a id="s-5adeebb411"></a>`400` | Bad Request |
| <a id="s-d23416f258"></a>`401` | Unauthorized |
| <a id="s-2d6c5f3aff"></a>`403` | Forbidden |
| <a id="s-3a833d8f38"></a>`500` | Internal Server Error |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog-ftp-adapter"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-b44fcc10a7"></a>[response 200 · content · application/json](#s-52ad3c0e19) | `cardinality · entries · operational_policy` | shared above |

## Maintained corroboration

### Related interface records

- [riverhog-ftp-adapter run](../cli/riverhog-ftp-adapter-run.md)

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
- [openapi:riverhog-ftp-adapter](../../../evidence/sources.md#src-c3a51ac29a) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

```json
{
  "application": "riverhog-ftp-adapter",
  "classification": "human-cli+json",
  "cli_commands": [
    "run"
  ],
  "client": "RiverhogFtpAdapterClient",
  "method": "POST",
  "operation_id": "run_ftp_adapter_pass",
  "path": "/v1/run",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

### Machine authority

- `/external_contract/http_openapi/riverhog-ftp-adapter/paths/~1v1~1run/post`

### Exact owned JSON

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
