# POST /v1/apps/{app}/keys/{key_id}/revoke

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:post-v1-apps-app-keys-key-id-revoke:de52e07c81 -->

Revoke App Key

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-34033d480c"></a>
- <a id="s-e43d5ad244"></a>`operationId`: `"revoke_app_key"`
- <a id="s-a81a4de2ec"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-0167227e49"></a>`summary`: `"Revoke App Key"`
- <a id="s-c9130b784e"></a>`tags`: `["apps"]`
- <a id="s-bfbb3e08e4"></a>`x-riverhog-permission-requirements`: `[{"any_of":["keys:manage"]}]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-8f6cfd79bd"></a>`app` | path | yes | not declared | type="string"; pattern="^[a-z0-9]+(?:-[a-z0-9]+)*$"; title="App" |
| <a id="s-aa96c7b448"></a>`key_id` | path | yes | not declared | type="string"; pattern="^[0-9a-f]{16}$"; title="Key Id" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-bb1d20bb4c"></a>`200` | Successful Response | application/json | [AppKeyOut](../http-schemas/schemas-appkeyout.md) | not declared |
| <a id="s-3ac0fd3dfb"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-cd3c295834"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-d24fbe56d8"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-719e1866d7"></a>`404` | Not Found | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `not_found` |
| <a id="s-39a6b94286"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=16; minimum=16; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{16}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-bfbab77e7d"></a>[parameter key_id](#s-aa96c7b448) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [piggity app key revoke](../../piggity/cli/piggity-app-key-revoke.md)
- [riverhog_client.ApiClient.revoke_app_key](../../riverhog-client/python/riverhog-client-apiclient-revoke-app-key.md)

### Referenced contract dossiers

- [schemas: AppKeyOut](../http-schemas/schemas-appkeyout.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-ab85d62601"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-635f8d4bd9"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **OpenAPI authority:** [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9)
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`
- **Handler:** [riverhog/src/riverhog_api/routers/apps.py::revoke_app_key](../../../../../../riverhog/src/riverhog_api/routers/apps.py#L282)

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
      "command": "app key revoke",
      "source": {
        "line": 1209,
        "module": "piggity.main",
        "path": "reference/riverhog/applications/piggity/src/piggity/main.py",
        "symbol": "app_key_revoke_cmd"
      }
    }
  ],
  "cli_commands": [
    "app key revoke"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.revoke_app_key",
      "source": {
        "line": 2132,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.revoke_app_key"
      }
    }
  ],
  "method": "POST",
  "operation_id": "revoke_app_key",
  "path": "/v1/apps/{app}/keys/{key_id}/revoke",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1apps~1{app}~1keys~1{key_id}~1revoke/post`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b0aed6d1261ccb167bcf9ccd0f02bf1388f65c850e91a855e0d3bdaf3f09d81d -->

```json
{
  "operationId": "revoke_app_key",
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
    },
    {
      "in": "path",
      "name": "key_id",
      "required": true,
      "schema": {
        "pattern": "^[0-9a-f]{16}$",
        "title": "Key Id",
        "type": "string"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/AppKeyOut"
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
  "summary": "Revoke App Key",
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
