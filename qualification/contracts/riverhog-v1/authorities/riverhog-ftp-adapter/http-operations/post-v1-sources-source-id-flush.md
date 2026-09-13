# POST /v1/sources/{source_id}/flush

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog-ftp-adapter:post-v1-sources-source-id-flush:bfa8577ddb -->

Flush

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-1a2048e455"></a>
- <a id="s-e25ea0a4db"></a>`operationId`: flush_ftp_adapter_source
- <a id="s-aef46c4863"></a>`summary`: Flush
- <a id="s-facdf1d6a2"></a>`security`: `[{"RiverhogFtpAdapterBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-208515f96a"></a>`source_id` | path | yes | type="string" |

### Responses

| Status | Description |
|---|---|
| <a id="s-010c427b4f"></a>`200` | Successful Response |
| <a id="s-e2ac7e3c6a"></a>`400` | Bad Request |
| <a id="s-86321cc684"></a>`401` | Unauthorized |
| <a id="s-ead99851f5"></a>`403` | Forbidden |
| <a id="s-c169c18007"></a>`500` | Internal Server Error |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog-ftp-adapter"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-3908ffb8fc"></a>[response 200 · content · application/json](#s-010c427b4f) | `cardinality · entries · operational_policy` | shared above |

## Maintained corroboration

### Related interface records

- [riverhog-ftp-adapter riverhog-ftp-adapter flush](../cli/riverhog-ftp-adapter-riverhog-ftp-adapter-flush.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-da1091c998"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-24983dd5c5"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog-ftp-adapter](../../../evidence/sources.md#src-c3a51ac29a) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Operation qualification evidence

This evidence proves maintained client, CLI, response-authority, and provider qualification without creating a second semantic operation.

```json
{
  "application": "riverhog-ftp-adapter",
  "classification": "human-cli+json",
  "cli_commands": [
    "flush"
  ],
  "client": "RiverhogFtpAdapterClient",
  "method": "POST",
  "operation_id": "flush_ftp_adapter_source",
  "path": "/v1/sources/{source_id}/flush",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

### Machine authority

- `/external_contract/http_openapi/riverhog-ftp-adapter/paths/~1v1~1sources~1{source_id}~1flush/post`

### Exact owned JSON

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
