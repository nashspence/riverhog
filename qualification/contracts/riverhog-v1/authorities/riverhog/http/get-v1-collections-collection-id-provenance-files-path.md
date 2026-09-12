# GET /v1/collections/{collection_id}/provenance/files/{path}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:get-v1-collections-collection-id-provenan-6ee3284e33:496ec197ee -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `collections` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collections~1{collection_id}~1provenance~1files~1{path}/get`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Related interface records

- [Operation parity: get_collection_file_provenance](../operation/operation-parity-get-collection-file-provenance.md)

## Referenced contract dossiers

- [schemas: CollectionFileProvenanceDetailOut](schemas-collectionfileprovenancedetailout.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=4096, minimum=1, reason=schema-maximum |

## Contract summary

- `operationId`: get_collection_file_provenance
- `summary`: Get Collection File Provenance
- `security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| `collection_id` | path | yes | integer |
| `path` | path | yes | string |

### Responses

| Status | Description |
|---|---|
| `200` | Successful Response |
| `400` | Bad Request |
| `401` | Unauthorized |
| `403` | Forbidden |
| `404` | Not Found |
| `409` | Conflict |
| `500` | Internal Server Error |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2090cd52fbd665b4bf3a656622c4118a5405e783866ed5b2d7c05b11010b2870 -->

```json
{
  "operationId": "get_collection_file_provenance",
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
      "in": "path",
      "name": "path",
      "required": true,
      "schema": {
        "allOf": [
          {
            "not": {
              "pattern": "(?:^|/)\\.{1,2}(?:/|$)"
            }
          },
          {
            "not": {
              "pattern": "^\\s|\\s$"
            }
          }
        ],
        "format": "riverhog-canonical-relpath-v1",
        "maxLength": 4096,
        "minLength": 1,
        "pattern": "^[^/\\\\]+(?:/[^/\\\\]+)*$",
        "title": "Path",
        "type": "string",
        "x-unicode-normalization": "NFC"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/CollectionFileProvenanceDetailOut"
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
  "summary": "Get Collection File Provenance",
  "tags": [
    "provenance"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "provenance:read"
      ]
    }
  ]
}
```
