# GET /v1/collection-upload-sessions/{collection_id}/work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-collection-upload-sessions-collection-id-work:7b0011a226 -->

Acquire Collection Upload Session Work

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-c49fcc285b"></a>
- <a id="s-c6a878ced1"></a>`operationId`: `"acquire_collection_upload_session_work"`
- <a id="s-5830733425"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-e31071e137"></a>`summary`: `"Acquire Collection Upload Session Work"`
- <a id="s-3608812045"></a>`tags`: `["collections"]`
- <a id="s-97c4cdb536"></a>`x-riverhog-interface`: `"client-only-primitive"`
- <a id="s-60ffdfa81f"></a>`x-riverhog-permission-requirements`: `[{"any_of":["collections:create"]}]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-4817272220"></a>`collection_id` | path | yes | not declared | type="integer"; minimum=1; title="Collection Id" |
| <a id="s-3ac16348cf"></a>`limit` | query | no | `16` | type="integer"; minimum=1; maximum=64; title="Limit" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-f83a0a8071"></a>`200` | Successful Response | application/json | [CollectionUploadWorkBatchDocument](../http-schemas/schemas-collectionuploadworkbatchdocument.md) | not declared |
| <a id="s-13fd040651"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-a3a786ddf9"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-9b1524bd4e"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-9dd14f9f96"></a>`404` | Not Found | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `not_found` |
| <a id="s-9d7cfc09e9"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-577bda457d"></a>[parameter limit](#s-3ac16348cf) | `value · schema-value · contract_max` | shared above |

## Maintained corroboration

### Related interface records

- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)
- [riverhog_client.ApiClient.acquire_collection_upload_session_work](../../riverhog-client/python/riverhog-client-apiclient-acquire-collection-upload-session-work.md)

### Referenced contract elements

- [schemas: CollectionUploadWorkBatchDocument](../http-schemas/schemas-collectionuploadworkbatchdocument.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-ce19a922fd"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-a91e16b5bb"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/collections.py::acquire\_collection\_upload\_session\_work](../../../../../../riverhog/src/riverhog_api/routers/collections.py#L545)

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

<details>
<summary>Exact structural binding record</summary>

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_bindings": [
    {
      "command": "collection upload start",
      "executable": "piggity",
      "result_identity": "piggity-cli-result/collection/upload/start/v1",
      "source": {
        "line": 2170,
        "module": "piggity.main",
        "path": "reference/riverhog/applications/piggity/src/piggity/main.py",
        "symbol": "upload_cmd"
      }
    }
  ],
  "cli_commands": [
    "collection upload start"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.acquire_collection_upload_session_work",
      "source": {
        "line": 1483,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.acquire_collection_upload_session_work"
      }
    }
  ],
  "method": "GET",
  "operation_id": "acquire_collection_upload_session_work",
  "path": "/v1/collection-upload-sessions/{collection_id}/work",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-upload-sessions~1{collection_id}~1work/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a7b7066a0533582660a7058aaa74180c78e9fd766403f5200ac8e4c0d772a890 -->

```json
{
  "operationId": "acquire_collection_upload_session_work",
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
      "name": "limit",
      "required": false,
      "schema": {
        "default": 16,
        "maximum": 64,
        "minimum": 1,
        "title": "Limit",
        "type": "integer"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/CollectionUploadWorkBatchDocument"
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
  "summary": "Acquire Collection Upload Session Work",
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
