# PATCH /v1/collection-upload-sessions/{collection_id}/provenance/journals/{journal_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:patch-v1-collection-upload-sessions-colle-7eff7dfe3e:43f71e064a -->

Append Collection Upload Session Provenance Journal

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [collection-upload-sessions](families/collection-upload-sessions/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-841e59e9d9"></a>
- <a id="s-980f5225e3"></a>`operationId`: append_collection_upload_session_provenance_journal
- <a id="s-8e8e0f9473"></a>`summary`: Append Collection Upload Session Provenance Journal
- <a id="s-ae48ec2ae7"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-19150eead7"></a>`collection_id` | path | yes | type="integer"; minimum=1 |
| <a id="s-c08d475e33"></a>`journal_id` | path | yes | type="string"; pattern="^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$" |
| <a id="s-e9674a110d"></a>`Upload-Offset` | header | yes | type="integer"; minimum=0 |
| <a id="s-c3c2815fa2"></a>`Content-Length` | header | yes | type="integer"; minimum=1; maximum=1048576 |

### <a id="s-7a66b60cea"></a>Request body

`{"content": {"application/json-seq": {"schema": {"format": "binary", "type": "string"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| <a id="s-14c3eb722c"></a>`200` | Successful Response |
| <a id="s-de30d6047a"></a>`400` | Bad Request |
| <a id="s-8245d6d669"></a>`401` | Unauthorized |
| <a id="s-d6d6d10f2f"></a>`403` | Forbidden |
| <a id="s-8895a08cc0"></a>`404` | Not Found |
| <a id="s-cbbd0cae8d"></a>`409` | Conflict |
| <a id="s-f09b3cb539"></a>`411` | Length Required |
| <a id="s-5d3d3ad6de"></a>`500` | Internal Server Error |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1048576; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-1ec924e50b"></a>[parameter Content-Length](#s-c3c2815fa2) | `value · schema-value · contract_max` | shared above |

## Maintained corroboration

### Related interface records

- [Operation parity: append_collection_upload_session_provenance_journal](../operation/operation-parity-append-collection-upload-session-provenance-journal.md)

### Referenced contract dossiers

- [schemas: CollectionUploadProvenanceJournalStatusDocument](schemas-collectionuploadprovenancejournalstatusdocument.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)

## Governing policies

- <a id="pa-677a6685bd"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-edfc9091b0"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-upload-sessions~1{collection_id}~1provenance~1journals~1{journal_id}/patch`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ce8f5c831863a3efe9d383cb20c619924e0d0a29ade9d2d03d77da12e0e1191f -->

```json
{
  "operationId": "append_collection_upload_session_provenance_journal",
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
      "name": "Upload-Offset",
      "required": true,
      "schema": {
        "minimum": 0,
        "title": "Upload-Offset",
        "type": "integer"
      }
    },
    {
      "description": "Exact bounded append length in bytes.",
      "in": "header",
      "name": "Content-Length",
      "required": true,
      "schema": {
        "maximum": 1048576,
        "minimum": 1,
        "type": "integer"
      }
    }
  ],
  "requestBody": {
    "content": {
      "application/json-seq": {
        "schema": {
          "format": "binary",
          "type": "string"
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
            "$ref": "#/components/schemas/CollectionUploadProvenanceJournalStatusDocument"
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
        "conflict"
      ]
    },
    "411": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Length Required",
      "x-riverhog-error-codes": [
        "length_required"
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
  "summary": "Append Collection Upload Session Provenance Journal",
  "tags": [
    "collections"
  ],
  "x-riverhog-interface": "client-only-primitive",
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "collections:create"
      ]
    }
  ]
}
```
