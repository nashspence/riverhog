# POST /v1/sources/{source_id}/flush

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog-ftp-adapter:post-v1-sources-source-id-flush:c19398bc43 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-ftp-adapter` |
| Interface | `http` |
| Family | `sources` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog-ftp-adapter/paths/~1v1~1sources~1{source_id}~1flush/post`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog-ftp-adapter` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Related interface records

- [Operation parity: flush_ftp_adapter_source](../operation/operation-parity-flush-ftp-adapter-source.md)

## Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `operationId`: flush_ftp_adapter_source
- `summary`: Flush
- `security`: `[{"RiverhogFtpAdapterBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| `source_id` | path | yes | string |

### Responses

| Status | Description |
|---|---|
| `200` | Successful Response |
| `400` | Bad Request |
| `401` | Unauthorized |
| `403` | Forbidden |
| `500` | Internal Server Error |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9606950cef18df30221388969787db2bb44fcce1a0a93c83ad7e22630416bf30 -->

```json
{
  "operationId": "flush_ftp_adapter_source",
  "parameters": [
    {
      "in": "path",
      "name": "source_id",
      "required": true,
      "schema": {
        "title": "Source Id",
        "type": "string"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "additionalProperties": true,
            "title": "Response Flush Ftp Adapter Source",
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
  "summary": "Flush",
  "tags": [
    "operations"
  ]
}
```
