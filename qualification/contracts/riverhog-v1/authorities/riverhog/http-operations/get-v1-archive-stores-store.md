# GET /v1/archive/stores/{store}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-archive-stores-store:83ac927314 -->

Get Archive Store

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-54ed75001a"></a>
- <a id="s-e0da451e48"></a>`operationId`: `"get_archive_store"`
- <a id="s-0ebb68c0e7"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-cf1105a3ac"></a>`summary`: `"Get Archive Store"`
- <a id="s-b48afcf192"></a>`tags`: `["archive"]`
- <a id="s-54e9264cf4"></a>`x-riverhog-permission-requirements`: `[{"any_of":["archives:read"]}]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-7b748f31c6"></a>`store` | path | yes | not declared | type="string"; pattern="^[a-z0-9]+(?:-[a-z0-9]+)*$"; title="Store" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-0c14d5abd4"></a>`200` | Successful Response | application/json | [ArchiveStoreOut](../http-schemas/schemas-archivestoreout.md) | not declared |
| <a id="s-d77497a05a"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-40f929e655"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-ca9e03ec21"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-848bef10e0"></a>`404` | Not Found | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `not_found` |
| <a id="s-d42d32e2d5"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [a-riverhog-cli archive store show](../../a-riverhog-cli/cli/a-riverhog-cli-archive-store-show.md)
- [riverhog_client.ApiClient.get_archive_store](../../riverhog-client/python/riverhog-client-apiclient-get-archive-store.md)

### Referenced contract elements

- [schemas: ArchiveStoreOut](../http-schemas/schemas-archivestoreout.md)
- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)

## Governing policies

- <a id="pa-1497c05adc"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/archive.py::get\_archive\_store](../../../../../../riverhog/src/riverhog_api/routers/archive.py#L238)

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

<details>
<summary>Exact structural binding record</summary>

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_bindings": [
    {
      "command": "archive store show",
      "executable": "a-riverhog-cli",
      "result_identity": "a-riverhog-cli-result/archive/store/show/v1",
      "source": {
        "line": 2939,
        "module": "a_riverhog_cli.main",
        "path": "some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py",
        "symbol": "archive_store_show_cmd"
      }
    }
  ],
  "cli_commands": [
    "archive store show"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.get_archive_store",
      "source": {
        "line": 2084,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.get_archive_store"
      }
    }
  ],
  "method": "GET",
  "operation_id": "get_archive_store",
  "path": "/v1/archive/stores/{store}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1archive~1stores~1{store}/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 49e605af36f90381c31060d87177b10cd5688ae4ad838e3742becd208f4b3d34 -->

```json
{
  "operationId": "get_archive_store",
  "parameters": [
    {
      "in": "path",
      "name": "store",
      "required": true,
      "schema": {
        "pattern": "^[a-z0-9]+(?:-[a-z0-9]+)*$",
        "title": "Store",
        "type": "string"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ArchiveStoreOut"
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
    "404": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorOut"
          }
        }
      },
      "description": "Not Found",
      "x-riverhog-error-codes": [
        "not_found"
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
  "security": [
    {
      "HTTPBearer": []
    }
  ],
  "summary": "Get Archive Store",
  "tags": [
    "archive"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "archives:read"
      ]
    }
  ]
}
```

</details>
