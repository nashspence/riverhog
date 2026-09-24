# POST /v1/apps/{app}/keys/{key_id}/rotate

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:post-v1-apps-app-keys-key-id-rotate:c54abfcea8 -->

Rotate App Key

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-d90ab0fb69"></a>
- <a id="s-26c926881d"></a>`operationId`: `"rotate_app_key"`
- <a id="s-c7ea91120f"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-0ddea60dba"></a>`summary`: `"Rotate App Key"`
- <a id="s-38d55888cd"></a>`tags`: `["apps"]`
- <a id="s-2510bd18fe"></a>`x-riverhog-permission-requirements`: `[{"any_of":["keys:manage"]}]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-d7ff11fddc"></a>`app` | path | yes | not declared | type="string"; pattern="^[a-z0-9]+(?:-[a-z0-9]+)*$"; title="App" |
| <a id="s-f2fc54faec"></a>`key_id` | path | yes | not declared | type="string"; pattern="^[0-9a-f]{16}$"; title="Key Id" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-afcbe75da1"></a>`200` | Successful Response | application/json | [AppKeyCreatedOut](../http-schemas/schemas-appkeycreatedout.md) | not declared |
| <a id="s-4f38b01e2c"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-22688b96e0"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-27faa4ac6f"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-ec3b212fa2"></a>`404` | Not Found | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `not_found` |
| <a id="s-5b3ca4590c"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=16; minimum=16; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{16}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-63d8c2382c"></a>[parameter key_id](#s-f2fc54faec) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [a-riverhog-cli app key rotate](../../a-riverhog-cli/cli/a-riverhog-cli-app-key-rotate.md)
- [riverhog_client.ApiClient.rotate_app_key](../../riverhog-client/python/riverhog-client-apiclient-rotate-app-key.md)

### Referenced contract elements

- [schemas: AppKeyCreatedOut](../http-schemas/schemas-appkeycreatedout.md)
- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-c45536dd51"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-19066b6306"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/apps.py::rotate\_app\_key](../../../../../../riverhog/src/riverhog_api/routers/apps.py#L108)

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
      "command": "app key rotate",
      "executable": "a-riverhog-cli",
      "result_identity": "a-riverhog-cli-result/app/key/rotate/v1",
      "source": {
        "line": 1223,
        "module": "a_riverhog_cli.main",
        "path": "some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py",
        "symbol": "app_key_rotate_cmd"
      }
    }
  ],
  "cli_commands": [
    "app key rotate"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.rotate_app_key",
      "source": {
        "line": 2161,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.rotate_app_key"
      }
    }
  ],
  "method": "POST",
  "operation_id": "rotate_app_key",
  "path": "/v1/apps/{app}/keys/{key_id}/rotate",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1apps~1{app}~1keys~1{key_id}~1rotate/post`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a29e34410ef2baa37c2f9bd61f8b25cd50b5d42f160ccf4a76429d32fccd0f81 -->

```json
{
  "operationId": "rotate_app_key",
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
  "summary": "Rotate App Key",
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
