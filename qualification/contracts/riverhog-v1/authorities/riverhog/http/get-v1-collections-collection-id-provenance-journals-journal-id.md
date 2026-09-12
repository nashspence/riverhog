# GET /v1/collections/{collection_id}/provenance/journals/{journal_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:get-v1-collections-collection-id-provenan-3a9ce32406:fbdb931347 -->

Stream Collection Provenance Journal

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [collections](families/collections/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-134aff881b"></a>
- <a id="s-4c94154a95"></a>`operationId`: stream_collection_provenance_journal
- <a id="s-c4c0625828"></a>`summary`: Stream Collection Provenance Journal
- <a id="s-87943351ef"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-a4ef2bea49"></a>`collection_id` | path | yes | type="integer"; minimum=1 |
| <a id="s-c5835bbcc5"></a>`journal_id` | path | yes | type="string"; pattern="^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$" |
| <a id="s-4fba5c2893"></a>`Range` | header | no | anyOf=type="string" \| type="null" |
| <a id="s-c8fe3179f9"></a>`If-Match` | header | no | anyOf=type="string" \| type="null" |

### Responses

| Status | Description |
|---|---|
| <a id="s-92f2a107ee"></a>`200` | Exact immutable provenance journal. |
| <a id="s-97663fb9b7"></a>`206` | Exact immutable provenance journal byte range. |
| <a id="s-92337c450c"></a>`400` | Bad Request |
| <a id="s-d3af899133"></a>`401` | Unauthorized |
| <a id="s-a469d6dc90"></a>`403` | Forbidden |
| <a id="s-31ed9a3535"></a>`404` | Not Found |
| <a id="s-b74cf514a8"></a>`412` | Precondition Failed |
| <a id="s-2fff5173f2"></a>`428` | Precondition Required |
| <a id="s-2339147e1d"></a>`500` | Internal Server Error |

## Maintained corroboration

### Related interface records

- [Operation parity: stream_collection_provenance_journal](../operation/operation-parity-stream-collection-provenance-journal.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](schemas-errorresponse.md)

## Governing policies

- <a id="pa-c38fc8ddac"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collections~1{collection_id}~1provenance~1journals~1{journal_id}/get`

### Exact owned JSON

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
