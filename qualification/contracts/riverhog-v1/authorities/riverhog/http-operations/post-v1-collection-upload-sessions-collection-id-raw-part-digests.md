# POST /v1/collection-upload-sessions/{collection_id}/raw-part-digests

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:post-v1-collection-upload-sessions-collec-fffabb89d2:bb2ca4c27b -->

Register Collection Upload Session Raw Part Digests

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-aee096c8ac"></a>
- <a id="s-c5e60abce4"></a>`operationId`: `"register_collection_upload_session_raw_part_digests"`
- <a id="s-2fbf3a3327"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-93c8c73dd7"></a>`summary`: `"Register Collection Upload Session Raw Part Digests"`
- <a id="s-458f6d28e3"></a>`tags`: `["collections"]`
- <a id="s-c80ecbccb0"></a>`x-riverhog-interface`: `"client-only-primitive"`
- <a id="s-dea69d6a0b"></a>`x-riverhog-permission-requirements`: `[{"any_of":["collections:create"]}]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-30fea773af"></a>`collection_id` | path | yes | not declared | type="integer"; minimum=1; title="Collection Id" |

### <a id="s-a616ac0e21"></a>Request body

- `required`: `true`

| Media type | Schema |
|---|---|
| application/json | [CollectionUploadRawDigestBatchDocument](../http-schemas/schemas-collectionuploadrawdigestbatchdocument.md) |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-17836e8b2c"></a>`200` | Successful Response | application/json | [CollectionUploadRawDigestProgressDocument](../http-schemas/schemas-collectionuploadrawdigestprogressdocument.md) | not declared |
| <a id="s-3459ae1e9e"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-d9bb97b0c3"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-9db3fc3144"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-b91b130030"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)
- [riverhog_client.ApiClient.register_collection_upload_session_raw_part_digests](../../riverhog-client/python/riverhog-client-apiclient-register-collection-upload-session-raw-part-digests.md)

### Referenced contract elements

- [schemas: CollectionUploadRawDigestBatchDocument](../http-schemas/schemas-collectionuploadrawdigestbatchdocument.md)
- [schemas: CollectionUploadRawDigestProgressDocument](../http-schemas/schemas-collectionuploadrawdigestprogressdocument.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-4baea39d04"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/collections.py::register\_collection\_upload\_session\_raw\_part\_digests](../../../../../../riverhog/src/riverhog_api/routers/collections.py#L309)

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
      "public_identity": "riverhog_client.ApiClient.register_collection_upload_session_raw_part_digests",
      "source": {
        "line": 1233,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.register_collection_upload_session_raw_part_digests"
      }
    }
  ],
  "method": "POST",
  "operation_id": "register_collection_upload_session_raw_part_digests",
  "path": "/v1/collection-upload-sessions/{collection_id}/raw-part-digests",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "canonical-document"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-upload-sessions~1{collection_id}~1raw-part-digests/post`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 527571e9a892d14a93459cea9c3b7f5805caf028d194be405932cfaa3b0961b4 -->

```json
{
  "operationId": "register_collection_upload_session_raw_part_digests",
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
    }
  ],
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/CollectionUploadRawDigestBatchDocument"
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
            "$ref": "#/components/schemas/CollectionUploadRawDigestProgressDocument"
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
  "summary": "Register Collection Upload Session Raw Part Digests",
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
