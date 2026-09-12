# POST /v1/collection-processing-claims/{claim_id}/capabilities

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:post-v1-collection-processing-claims-clai-dfec4c2ebf:6a2f26e134 -->

Create Transform Capability

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-14e8cf6a72"></a>
- <a id="s-7b539cd145"></a>`operationId`: create_transform_capability
- <a id="s-33f526a015"></a>`summary`: Create Transform Capability
- <a id="s-90b4519db2"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-f5e6c98ff5"></a>`claim_id` | path | yes | type="string"; pattern="^[0-9a-f]{64}$" |

### <a id="s-00b1d345ce"></a>Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/TransformCapabilityCreateDocument"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-3f274725a6"></a>`200` | Successful Response |
| <a id="s-59c01857ab"></a>`400` | Bad Request |
| <a id="s-27b74b90dd"></a>`401` | Unauthorized |
| <a id="s-77e87db659"></a>`403` | Forbidden |
| <a id="s-761d51a50f"></a>`404` | Not Found |
| <a id="s-271ac1e521"></a>`409` | Conflict |
| <a id="s-7df5274677"></a>`500` | Internal Server Error |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-2a0921559c"></a>[parameter claim_id](#s-f5e6c98ff5) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [Operation parity: create_transform_capability](../operation/operation-parity-create-transform-capability.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: TransformCapabilityCreateDocument](schemas-transformcapabilitycreatedocument.md)
- [schemas: TransformCapabilityDocument](schemas-transformcapabilitydocument.md)

## Governing policies

- <a id="pa-5ca2193189"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-43a24ed65d"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-processing-claims~1{claim_id}~1capabilities/post`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8f17e09c060afbe39816756ab2472803b37c48f658bfdbe3674013e7d93fef49 -->

```json
{
  "operationId": "create_transform_capability",
  "parameters": [
    {
      "in": "path",
      "name": "claim_id",
      "required": true,
      "schema": {
        "pattern": "^[0-9a-f]{64}$",
        "title": "Claim Id",
        "type": "string"
      }
    }
  ],
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/TransformCapabilityCreateDocument"
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
            "$ref": "#/components/schemas/TransformCapabilityDocument"
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
        "conflict",
        "invalid_state"
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
  "summary": "Create Transform Capability",
  "tags": [
    "collection-workflows"
  ],
  "x-riverhog-interface": "client-only-primitive",
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "collection-transforms:execute"
      ]
    }
  ]
}
```
