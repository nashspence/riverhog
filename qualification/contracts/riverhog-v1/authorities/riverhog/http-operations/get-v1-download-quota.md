# GET /v1/download-quota

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-download-quota:7355f11485 -->

Get Download Quota

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-7294de1502"></a>
- <a id="s-f1d1326051"></a>`operationId`: get_download_quota
- <a id="s-2a4e51b181"></a>`summary`: Get Download Quota
- <a id="s-890e8605be"></a>`security`: `[{"HTTPBearer": []}]`

### Responses

| Status | Description |
|---|---|
| <a id="s-0b2951931e"></a>`200` | Successful Response |
| <a id="s-a87d3d6ab1"></a>`400` | Bad Request |
| <a id="s-91509b582b"></a>`401` | Unauthorized |
| <a id="s-24c09fefc6"></a>`403` | Forbidden |
| <a id="s-6bff31593a"></a>`404` | Not Found |
| <a id="s-dc3bea563b"></a>`429` | Too Many Requests |
| <a id="s-1965cf404b"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [piggity app key quota show](../../piggity/cli/piggity-app-key-quota-show.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: KeyDownloadQuotaOut](../http-schemas/schemas-keydownloadquotaout.md)

## Governing policies

- <a id="pa-154258b868"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Operation qualification evidence

This evidence proves maintained client, CLI, response-authority, and provider qualification without creating a second semantic operation.

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "app key quota show"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "get_download_quota",
  "path": "/v1/download-quota",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1download-quota/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8c5311ef3e34ab2135e9dcade123a6aa12b8b52eb738e4ab951291400fcdd9ab -->

```json
{
  "operationId": "get_download_quota",
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/KeyDownloadQuotaOut"
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
    "404": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Not Found",
      "x-riverhog-error-codes": [
        "not_found"
      ]
    },
    "429": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Too Many Requests",
      "x-riverhog-error-codes": [
        "download_allowance_exceeded"
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
      "HTTPBearer": []
    }
  ],
  "summary": "Get Download Quota",
  "tags": [
    "download quotas"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "retrieval:manage"
      ]
    }
  ]
}
```
