# POST /v1/run

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog-ftp-adapter:post-v1-run:bef97111d9 -->

Run Pass

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [http](index.md) |
| Family | [run](index.md#f-7869babba6) |
| Contract elements | 1 |
| Extent decisions | 1 |

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

- [Operation parity: run_ftp_adapter_pass](../operation/operation-parity-run-ftp-adapter-pass.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)

## Governing policies

- <a id="pa-b4ec76deb7"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-3cd5637e3c"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog-ftp-adapter](../../../evidence/sources.md#src-c3a51ac29a) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
