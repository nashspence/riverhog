# POST /v1/work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:stove0:post-v1-work:9f500d8faf -->

Create Work

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-4d80280945"></a>
- <a id="s-5d745b511d"></a>`operationId`: `"create_work"`
- <a id="s-9876a6f951"></a>`summary`: `"Create Work"`
- <a id="s-60c048ab54"></a>`tags`: `["work"]`

### <a id="s-d1afd5577b"></a>Request body

- `required`: `true`

| Media type | Schema |
|---|---|
| application/json | [WorkCreateIn](../http-schemas/schemas-workcreatein.md) |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-bc8a3b692a"></a>`201` | Successful Response | application/json | [WorkView](../http-schemas/schemas-workview.md) | not declared |
| <a id="s-91b5b5e2ec"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-b690a7d530"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-0a5928f843"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-563a5450db"></a>`404` | Not Found | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `not_found` |
| <a id="s-cb81ab1c0b"></a>`409` | Conflict | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `conflict` |
| <a id="s-3bdbaf0871"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [stove0 work create](../../stove0-client/cli/stove0-work-create.md)
- [stove0_api_client.Stove0ApiClient.create_work](../../stove0-api-client/python/stove0-api-client-stove0apiclient-create-work.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: WorkCreateIn](../http-schemas/schemas-workcreatein.md)
- [schemas: WorkView](../http-schemas/schemas-workview.md)

## Governing policies

- <a id="pa-523423cffe"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **OpenAPI authority:** [openapi:stove0](../../../evidence/sources.md#src-52e6e32124)
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`
- **Handler:** [reference/stove0/application/server/src/stove0_api/app.py::create_app.<locals>.create_work](../../../../../../reference/stove0/application/server/src/stove0_api/app.py#L700)

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

<details>
<summary>Exact structural binding record</summary>

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_bindings": [
    {
      "command": "work create",
      "source": {
        "line": 295,
        "module": "stove0_cli.main",
        "path": "reference/stove0/application/client/src/stove0_cli/main.py",
        "symbol": "create_work"
      }
    }
  ],
  "cli_commands": [
    "work create"
  ],
  "client": "Stove0ApiClient",
  "client_bindings": [
    {
      "public_identity": "stove0_api_client.Stove0ApiClient.create_work",
      "source": {
        "line": 255,
        "module": "stove0_api_client.client",
        "path": "reference/stove0/packages/api-client/src/stove0_api_client/client.py",
        "symbol": "Stove0ApiClient.create_work"
      }
    }
  ],
  "method": "POST",
  "operation_id": "create_work",
  "path": "/v1/work",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1work/post`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 75c389451a78879c7121549f8e50750e51abde916e90b5f2da1ff2c5b6e70714 -->

```json
{
  "operationId": "create_work",
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/WorkCreateIn"
        }
      }
    },
    "required": true
  },
  "responses": {
    "201": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/WorkView"
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
    "409": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Conflict",
      "x-riverhog-error-codes": [
        "conflict"
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
  "summary": "Create Work",
  "tags": [
    "work"
  ]
}
```

</details>
