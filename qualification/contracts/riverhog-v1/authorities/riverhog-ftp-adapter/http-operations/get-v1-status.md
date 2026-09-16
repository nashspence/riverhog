# GET /v1/status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog-ftp-adapter:get-v1-status:ba1a977135 -->

Status

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-aad3e3439b"></a>
- <a id="s-830a485e4c"></a>`operationId`: get_ftp_adapter_status
- <a id="s-e0703c191b"></a>`summary`: Status
- <a id="s-9eb6d4f112"></a>`security`: `[{"RiverhogFtpAdapterBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-702a3a9511"></a>`page_size` | query | no | type="integer"; minimum=1; maximum=100 |
| <a id="s-9a0968104e"></a>`page_token` | query | no | anyOf=type="string"; minLength=1; maxLength=120 \| type="null" |

### Responses

| Status | Description |
|---|---|
| <a id="s-de7ccd665a"></a>`200` | Successful Response |
| <a id="s-ce6b0b2450"></a>`400` | Bad Request |
| <a id="s-ca2c31b304"></a>`401` | Unauthorized |
| <a id="s-fa00d2585f"></a>`403` | Forbidden |
| <a id="s-8fcac3eae8"></a>`500` | Internal Server Error |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog-ftp-adapter"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-d4ba6cd4c9"></a>[response 200 · content · application/json](#s-de7ccd665a) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/status](#s-aad3e3439b) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-c027b22e80"></a>[parameter page_size](#s-702a3a9511) | `value · schema-value · contract_max` | maximum=100 |
| <a id="s-f6f8d4c4d8"></a>[parameter page_token · string value](#s-9a0968104e) | `length · characters · contract_max` | maximum=120 |

### Progression evidence and open obligations

These are candidate test bindings. Group-wide progression claims remain unestablished; inspect the test scopes before applying a result to this contract.

- [riverhog-ftp-adapter-status-progression/v1](../../../evidence/sources.md#e-5707b3a2d3-98357b3ef7)

## Maintained corroboration

### Related interface records

- [riverhog-ftp-adapter status](../cli/riverhog-ftp-adapter-status.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-8a5e37e320"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-3de630cc34"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-1aad6e2eb6"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-a672bf89d6"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

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
    "status"
  ],
  "client": "RiverhogFtpAdapterClient",
  "method": "GET",
  "operation_id": "get_ftp_adapter_status",
  "path": "/v1/status",
  "provider_evidence": null,
  "read_collection": {
    "default_page_size": 25,
    "kind": "mutable-browse",
    "maximum_page_size": 100,
    "next_page_token_field": "next_page_token",
    "page_size_parameter": "page_size",
    "page_token_parameter": "page_token"
  },
  "response_authority": "http-json"
}
```

### Machine authority

- `/external_contract/http_openapi/riverhog-ftp-adapter/paths/~1v1~1status/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2ef0b2fa72d8f06e46d0863bd34f0c35b1345f1e3b99b5e77e33984f7df5a4b6 -->

```json
{
  "operationId": "get_ftp_adapter_status",
  "parameters": [
    {
      "in": "query",
      "name": "page_size",
      "required": false,
      "schema": {
        "default": 25,
        "maximum": 100,
        "minimum": 1,
        "title": "Page Size",
        "type": "integer"
      }
    },
    {
      "in": "query",
      "name": "page_token",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "maxLength": 120,
            "minLength": 1,
            "type": "string"
          },
          {
            "type": "null"
          }
        ],
        "title": "Page Token"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "additionalProperties": true,
            "title": "Response Get Ftp Adapter Status",
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
  "summary": "Status",
  "tags": [
    "service"
  ],
  "x-riverhog-read-collection": {
    "default_page_size": 25,
    "kind": "mutable-browse",
    "maximum_page_size": 100,
    "next_page_token_field": "next_page_token",
    "page_size_parameter": "page_size",
    "page_token_parameter": "page_token"
  }
}
```
