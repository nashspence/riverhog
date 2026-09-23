# PATCH /v1/collection-upload-sessions/{collection_id}/provenance/journals/{journal_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:patch-v1-collection-upload-sessions-colle-7eff7dfe3e:45fcdad648 -->

Append Collection Upload Session Provenance Journal

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-841e59e9d9"></a>
- <a id="s-980f5225e3"></a>`operationId`: `"append_collection_upload_session_provenance_journal"`
- <a id="s-ae48ec2ae7"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-8e8e0f9473"></a>`summary`: `"Append Collection Upload Session Provenance Journal"`
- <a id="s-f1b70ad30c"></a>`tags`: `["collections"]`
- <a id="s-1008f99020"></a>`x-riverhog-interface`: `"client-only-primitive"`
- <a id="s-90fae61a33"></a>`x-riverhog-permission-requirements`: `[{"any_of":["collections:create"]}]`

### Parameters

| Name | In | Required | Default | Schema | Description |
|---|---|---:|---|---|---|
| <a id="s-19150eead7"></a>`collection_id` | path | yes | not declared | type="integer"; minimum=1; title="Collection Id" |  |
| <a id="s-c08d475e33"></a>`journal_id` | path | yes | not declared | type="string"; pattern="^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"; title="Journal Id" |  |
| <a id="s-e9674a110d"></a>`Upload-Offset` | header | yes | not declared | type="integer"; minimum=0; title="Upload-Offset" |  |
| <a id="s-c3c2815fa2"></a>`Content-Length` | header | yes | not declared | type="integer"; minimum=1; maximum=1048576 | Exact bounded append length in bytes. |

### <a id="s-7a66b60cea"></a>Request body

- `required`: `true`

| Media type | Schema |
|---|---|
| application/json-seq | type="string"; format="binary" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-14c3eb722c"></a>`200` | Successful Response | application/json | [CollectionUploadProvenanceJournalStatusDocument](../http-schemas/schemas-collectionuploadprovenancejournalstatusdocument.md) | not declared |
| <a id="s-de30d6047a"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-8245d6d669"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-d6d6d10f2f"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-8895a08cc0"></a>`404` | Not Found | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `not_found` |
| <a id="s-cbbd0cae8d"></a>`409` | Conflict | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `conflict` |
| <a id="s-f09b3cb539"></a>`411` | Length Required | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `length_required` |
| <a id="s-5d3d3ad6de"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1048576; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-1ec924e50b"></a>[parameter Content-Length](#s-c3c2815fa2) | `value · schema-value · contract_max` | shared above |

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient.append_collection_upload_session_provenance_journal](../../riverhog-client/python/riverhog-client-apiclient-append-collection-upload-session-provenance-journal.md)

### Referenced contract elements

- [schemas: CollectionUploadProvenanceJournalStatusDocument](../http-schemas/schemas-collectionuploadprovenancejournalstatusdocument.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-3f560a87d4"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-10a41d3d21"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/collections.py::append\_collection\_upload\_session\_provenance\_journal](../../../../../../riverhog/src/riverhog_api/routers/collections.py#L344)

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

<details>
<summary>Exact structural binding record</summary>

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_bindings": [],
  "cli_commands": [],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.append_collection_upload_session_provenance_journal",
      "source": {
        "line": 1278,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.append_collection_upload_session_provenance_journal"
      }
    }
  ],
  "method": "PATCH",
  "operation_id": "append_collection_upload_session_provenance_journal",
  "path": "/v1/collection-upload-sessions/{collection_id}/provenance/journals/{journal_id}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-upload-sessions~1{collection_id}~1provenance~1journals~1{journal_id}/patch`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
