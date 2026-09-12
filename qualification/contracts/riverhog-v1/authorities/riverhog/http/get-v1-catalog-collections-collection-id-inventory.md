# GET /v1/catalog/collections/{collection_id}/inventory

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:get-v1-catalog-collections-collection-id-inventory:fffa90f23d -->

Get Portable Collection Inventory

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [catalog](families/catalog/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-cbec0de737"></a>
- <a id="s-209130ba65"></a>`operationId`: get_portable_collection_inventory
- <a id="s-b2ccb379bb"></a>`summary`: Get Portable Collection Inventory
- <a id="s-dd5b3ff2f4"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-d4fa758cae"></a>`collection_id` | path | yes | type="integer"; minimum=1 |
| <a id="s-d8cb38ecac"></a>`cursor` | query | no | anyOf=type="string"; minLength=1; maxLength=8192 \| type="null" |
| <a id="s-86388b9ad1"></a>`limit` | query | no | type="integer"; minimum=1; maximum=1000 |
| <a id="s-7620688610"></a>`If-Match` | header | no | anyOf=type="string"; pattern="^\"[0-9a-f]{64}\"$" \| type="null" |

### Responses

| Status | Description |
|---|---|
| <a id="s-4c657abee9"></a>`200` | Successful Response |
| <a id="s-0acee23acf"></a>`400` | Bad Request |
| <a id="s-693d94ba8f"></a>`401` | Unauthorized |
| <a id="s-f1be810f81"></a>`403` | Forbidden |
| <a id="s-68d71c6c15"></a>`404` | Not Found |
| <a id="s-b8b057a58d"></a>`412` | Precondition Failed |
| <a id="s-3158a7f072"></a>`428` | Precondition Required |
| <a id="s-7940c10130"></a>`500` | Internal Server Error |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"authority":"portable-collection-inventory","cursor_parameter":"cursor","kind":"exact-set-page","limit_parameter":"limit","validator_header":"If-Match"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/catalog/collections/{collection_id}/inventory](#s-cbec0de737) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-e0db9ba9c7"></a>[parameter cursor · string value](#s-d8cb38ecac) | `length · characters · contract_max` | maximum=8192 |
| <a id="s-b623c0150e"></a>[parameter limit](#s-86388b9ad1) | `value · schema-value · contract_max` | maximum=1000 |

## Maintained corroboration

### Related interface records

- [Operation parity: get_portable_collection_inventory](../operation/operation-parity-get-portable-collection-inventory.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)
- [schemas: PortableCollectionInventoryPage](schemas-portablecollectioninventorypage.md)

## Governing policies

- <a id="pa-db22d23c5b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-4df18b4714"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-84d499ea19"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1catalog~1collections~1{collection_id}~1inventory/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3422ec3acc10c1715c4b1f195c211da88d6a6c391f2aebc2903ad8d88beaf538 -->

```json
{
  "operationId": "get_portable_collection_inventory",
  "parameters": [
    {
      "in": "path",
      "name": "collection_id",
      "required": true,
      "schema": {
        "minimum": 1,
        "title": "Collection Id",
        "type": "integer"
      }
    },
    {
      "in": "query",
      "name": "cursor",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "maxLength": 8192,
            "minLength": 1,
            "type": "string"
          },
          {
            "type": "null"
          }
        ],
        "title": "Cursor"
      }
    },
    {
      "in": "query",
      "name": "limit",
      "required": false,
      "schema": {
        "default": 100,
        "maximum": 1000,
        "minimum": 1,
        "title": "Limit",
        "type": "integer"
      }
    },
    {
      "in": "header",
      "name": "If-Match",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "pattern": "^\"[0-9a-f]{64}\"$",
            "type": "string"
          },
          {
            "type": "null"
          }
        ],
        "title": "If-Match"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/PortableCollectionInventoryPage"
          }
        }
      },
      "description": "Successful Response",
      "headers": {
        "ETag": {
          "description": "Strong identity of the immutable inventory authority.",
          "schema": {
            "pattern": "^\"[0-9a-f]{64}\"$",
            "type": "string"
          }
        }
      }
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
    "412": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Precondition Failed",
      "x-riverhog-error-codes": [
        "precondition_failed"
      ]
    },
    "428": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Precondition Required",
      "x-riverhog-error-codes": [
        "precondition_required"
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
  "summary": "Get Portable Collection Inventory",
  "tags": [
    "catalog"
  ],
  "x-riverhog-interface": "standard-tool/protocol",
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "catalog:read"
      ]
    }
  ],
  "x-riverhog-read-collection": {
    "authority": "portable-collection-inventory",
    "cursor_parameter": "cursor",
    "kind": "exact-set-page",
    "limit_parameter": "limit",
    "validator_header": "If-Match"
  }
}
```
