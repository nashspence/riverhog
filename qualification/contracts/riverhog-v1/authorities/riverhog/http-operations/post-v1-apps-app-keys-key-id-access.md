# POST /v1/apps/{app}/keys/{key_id}/access

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:post-v1-apps-app-keys-key-id-access:a4719ca7a0 -->

Add App Key Access

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-4f11ed8802"></a>
- <a id="s-d9bdbbb685"></a>`operationId`: `"add_app_key_access"`
- <a id="s-9583721fc7"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-8b65ebef16"></a>`summary`: `"Add App Key Access"`
- <a id="s-8142d1f569"></a>`tags`: `["apps"]`
- <a id="s-00f5a863e4"></a>`x-riverhog-permission-requirements`: `[{"any_of":["keys:manage"]}]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-63893cdb93"></a>`app` | path | yes | not declared | type="string"; pattern="^[a-z0-9]+(?:-[a-z0-9]+)*$"; title="App" |
| <a id="s-08375718d6"></a>`key_id` | path | yes | not declared | type="string"; pattern="^[0-9a-f]{16}$"; title="Key Id" |

### <a id="s-e7ba4e90d6"></a>Request body

- `required`: `true`

| Media type | Schema |
|---|---|
| application/json | [MutateAppAccessRequest](../http-schemas/schemas-mutateappaccessrequest.md) |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-f1906aba47"></a>`200` | Successful Response | application/json | [AppAccessSetOut](../http-schemas/schemas-appaccesssetout.md) | not declared |
| <a id="s-d51e8deadd"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-70b7679316"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-23814ad55b"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-564a26372f"></a>`404` | Not Found | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `not_found` |
| <a id="s-ab82137c69"></a>`409` | Conflict | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `conflict` |
| <a id="s-4b5155e96b"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=16; minimum=16; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{16}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-96e23c6645"></a>[parameter key_id](#s-08375718d6) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [a-riverhog-cli app key access add](../../a-riverhog-cli/cli/a-riverhog-cli-app-key-access-add.md)
- [riverhog_client.ApiClient.add_app_key_access](../../riverhog-client/python/riverhog-client-apiclient-add-app-key-access.md)

### Referenced contract elements

- [schemas: AppAccessSetOut](../http-schemas/schemas-appaccesssetout.md)
- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)
- [schemas: MutateAppAccessRequest](../http-schemas/schemas-mutateappaccessrequest.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-6132a688db"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-01a3d0de0c"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/apps.py::add\_app\_key\_access](../../../../../../riverhog/src/riverhog_api/routers/apps.py#L199)

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
      "command": "app key access add",
      "executable": "a-riverhog-cli",
      "result_identity": "a-riverhog-cli-result/app/key/access/add/v1",
      "source": {
        "line": 1318,
        "module": "a_riverhog_cli.main",
        "path": "some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py",
        "symbol": "app_key_access_add_cmd"
      }
    }
  ],
  "cli_commands": [
    "app key access add"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.add_app_key_access",
      "source": {
        "line": 2232,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.add_app_key_access"
      }
    }
  ],
  "method": "POST",
  "operation_id": "add_app_key_access",
  "path": "/v1/apps/{app}/keys/{key_id}/access",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1apps~1{app}~1keys~1{key_id}~1access/post`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e7370aeaa25dd26be77b05d607471c521d59462c6820eb507cd06a4dbb553cb6 -->

```json
{
  "operationId": "add_app_key_access",
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
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/MutateAppAccessRequest"
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
            "$ref": "#/components/schemas/AppAccessSetOut"
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
    "409": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorOut"
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
  "summary": "Add App Key Access",
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
