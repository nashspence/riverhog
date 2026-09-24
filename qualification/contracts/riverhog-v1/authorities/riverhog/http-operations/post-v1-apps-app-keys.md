# POST /v1/apps/{app}/keys

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:post-v1-apps-app-keys:3255ff34be -->

Create App Key

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-26f945d4a6"></a>
- <a id="s-e51936213d"></a>`operationId`: `"create_app_key"`
- <a id="s-24642ac73a"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-26529c3c51"></a>`summary`: `"Create App Key"`
- <a id="s-2e19dfcfc3"></a>`tags`: `["apps"]`
- <a id="s-18b4b8418d"></a>`x-riverhog-permission-requirements`: `[{"any_of":["keys:manage"]}]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-78a97764ff"></a>`app` | path | yes | not declared | type="string"; pattern="^[a-z0-9]+(?:-[a-z0-9]+)*$"; title="App" |

### <a id="s-a8bf91946a"></a>Request body

- `required`: `true`

| Media type | Schema |
|---|---|
| application/json | [CreateAppKeyRequest](../http-schemas/schemas-createappkeyrequest.md) |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-642e16127e"></a>`200` | Successful Response | application/json | [AppKeyCreatedOut](../http-schemas/schemas-appkeycreatedout.md) | not declared |
| <a id="s-655c2f197f"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-4e322efdee"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-3d6bf9afd5"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-defe02f120"></a>`404` | Not Found | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `not_found` |
| <a id="s-81f7f7d6ec"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [a-riverhog-cli app key create](../../a-riverhog-cli/cli/a-riverhog-cli-app-key-create.md)
- [riverhog_client.ApiClient.create_app_key](../../riverhog-client/python/riverhog-client-apiclient-create-app-key.md)

### Referenced contract elements

- [schemas: AppKeyCreatedOut](../http-schemas/schemas-appkeycreatedout.md)
- [schemas: CreateAppKeyRequest](../http-schemas/schemas-createappkeyrequest.md)
- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)

## Governing policies

- <a id="pa-ee1ce7f1e6"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/apps.py::create\_app\_key](../../../../../../riverhog/src/riverhog_api/routers/apps.py#L87)

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
      "command": "app key create",
      "executable": "a-riverhog-cli",
      "result_identity": "a-riverhog-cli-result/app/key/create/v1",
      "source": {
        "line": 1135,
        "module": "a_riverhog_cli.main",
        "path": "some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py",
        "symbol": "app_key_create_cmd"
      }
    }
  ],
  "cli_commands": [
    "app key create"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.create_app_key",
      "source": {
        "line": 2116,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.create_app_key"
      }
    }
  ],
  "method": "POST",
  "operation_id": "create_app_key",
  "path": "/v1/apps/{app}/keys",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1apps~1{app}~1keys/post`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5dd39692015615df1aa0c61455ecb55e2fbb5ecb19f904748485477c6b4b66b7 -->

```json
{
  "operationId": "create_app_key",
  "parameters": [
    {
      "in": "path",
      "name": "app",
      "required": true,
      "schema": {
        "pattern": "^[a-z0-9]+(?:-[a-z0-9]+)*$",
        "title": "App",
        "type": "string"
      }
    }
  ],
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/CreateAppKeyRequest"
        }
      }
    },
    "required": true
  },
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/AppKeyCreatedOut"
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
  "summary": "Create App Key",
  "tags": [
    "apps"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "keys:manage"
      ]
    }
  ]
}
```

</details>
