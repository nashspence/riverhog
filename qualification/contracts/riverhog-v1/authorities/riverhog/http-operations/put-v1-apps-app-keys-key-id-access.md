# PUT /v1/apps/{app}/keys/{key_id}/access

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:put-v1-apps-app-keys-key-id-access:eb3e068ad0 -->

Replace App Key Access

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-31f6b63f56"></a>
- <a id="s-26371cc086"></a>`operationId`: replace_app_key_access
- <a id="s-c63a708517"></a>`summary`: Replace App Key Access
- <a id="s-ca3cc12e8a"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-66b22104b4"></a>`app` | path | yes | type="string"; pattern="^[a-z0-9]+(?:-[a-z0-9]+)*$" |
| <a id="s-92485a317d"></a>`key_id` | path | yes | type="string"; pattern="^[0-9a-f]{16}$" |

### <a id="s-8b2efdc2d2"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/ReplaceAppAccessRequest"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-6ffd4b8519"></a>`200` | Successful Response |
| <a id="s-650ad6fa8a"></a>`400` | Bad Request |
| <a id="s-58207d2780"></a>`401` | Unauthorized |
| <a id="s-4fadecf513"></a>`403` | Forbidden |
| <a id="s-de2d12b92a"></a>`404` | Not Found |
| <a id="s-4917871d1e"></a>`500` | Internal Server Error |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=16; minimum=16; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{16}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-f0f49b2600"></a>[parameter key_id](#s-92485a317d) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [piggity app key access set](../../piggity/cli/piggity-app-key-access-set.md)

### Referenced contract dossiers

- [schemas: AppAccessSetOut](../http-schemas/schemas-appaccesssetout.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: ReplaceAppAccessRequest](../http-schemas/schemas-replaceappaccessrequest.md)

## Governing policies

- <a id="pa-e1ebaf39da"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-b568ce4331"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

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
    "app key access set"
  ],
  "client": "ApiClient",
  "method": "PUT",
  "operation_id": "replace_app_key_access",
  "path": "/v1/apps/{app}/keys/{key_id}/access",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1apps~1{app}~1keys~1{key_id}~1access/put`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 88e3b7fb49a9144e254f8e612efbcba1d3fdfdc4e90f8bdb26d355a6dbffaae9 -->

```json
{
  "operationId": "replace_app_key_access",
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
          "$ref": "#/components/schemas/ReplaceAppAccessRequest"
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
  "summary": "Replace App Key Access",
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
