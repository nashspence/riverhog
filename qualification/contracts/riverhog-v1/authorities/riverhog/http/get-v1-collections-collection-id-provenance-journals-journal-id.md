# GET /v1/collections/{collection_id}/provenance/journals/{journal_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:get-v1-collections-collection-id-provenan-3a9ce32406:fbdb931347 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `collections` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collections~1{collection_id}~1provenance~1journals~1{journal_id}/get`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Related interface records

- [Operation parity: stream_collection_provenance_journal](../operation/operation-parity-stream-collection-provenance-journal.md)

## Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)

## Contract summary

- `operationId`: stream_collection_provenance_journal
- `summary`: Stream Collection Provenance Journal
- `security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| `collection_id` | path | yes | integer |
| `journal_id` | path | yes | string |
| `Range` | header | no | object (2 fields) |
| `If-Match` | header | no | object (2 fields) |

### Responses

| Status | Description |
|---|---|
| `200` | Exact immutable provenance journal. |
| `206` | Exact immutable provenance journal byte range. |
| `400` | Bad Request |
| `401` | Unauthorized |
| `403` | Forbidden |
| `404` | Not Found |
| `412` | Precondition Failed |
| `428` | Precondition Required |
| `500` | Internal Server Error |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 99f0bd739aeecce1c56c5b63ebe42ac43deaf07949c3724ec0f76dfc12256ecf -->

```json
{
  "operationId": "stream_collection_provenance_journal",
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
      "name": "journal_id",
      "required": true,
      "schema": {
        "pattern": "^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        "title": "Journal Id",
        "type": "string"
      }
    },
    {
      "in": "header",
      "name": "Range",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "type": "string"
          },
          {
            "type": "null"
          }
        ],
        "title": "Range"
      }
    },
    {
      "in": "header",
      "name": "If-Match",
      "required": false,
      "schema": {
        "anyOf": [
          {
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
        "application/json-seq": {
          "schema": {
            "format": "binary",
            "type": "string"
          }
        }
      },
      "description": "Exact immutable provenance journal.",
      "headers": {
        "Accept-Ranges": {
          "required": true,
          "schema": {
            "const": "bytes",
            "type": "string"
          }
        },
        "Content-Length": {
          "description": "Exact response-body length in bytes.",
          "required": true,
          "schema": {
            "minimum": 0,
            "type": "integer"
          }
        },
        "ETag": {
          "description": "Quoted SHA-256 identity of the journal bytes.",
          "required": true,
          "schema": {
            "pattern": "^\"[0-9a-f]{64}\"$",
            "type": "string"
          }
        }
      }
    },
    "206": {
      "content": {
        "application/json-seq": {
          "schema": {
            "format": "binary",
            "type": "string"
          }
        }
      },
      "description": "Exact immutable provenance journal byte range.",
      "headers": {
        "Accept-Ranges": {
          "required": true,
          "schema": {
            "const": "bytes",
            "type": "string"
          }
        },
        "Content-Length": {
          "required": true,
          "schema": {
            "type": "integer"
          }
        },
        "Content-Range": {
          "required": true,
          "schema": {
            "type": "string"
          }
        },
        "ETag": {
          "required": true,
          "schema": {
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
  "summary": "Stream Collection Provenance Journal",
  "tags": [
    "provenance"
  ],
  "x-riverhog-interface": "client-only-primitive",
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "provenance:export"
      ]
    }
  ]
}
```
