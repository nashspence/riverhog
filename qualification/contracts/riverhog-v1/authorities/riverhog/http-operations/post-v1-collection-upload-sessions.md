# POST /v1/collection-upload-sessions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:post-v1-collection-upload-sessions:7ca5956980 -->

Create Or Resume Collection Upload Session

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-179e7e60d9"></a>
- <a id="s-fff99c5076"></a>`operationId`: `"create_or_resume_collection_upload_session"`
- <a id="s-3c88b322fa"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-ad6863fb60"></a>`summary`: `"Create Or Resume Collection Upload Session"`
- <a id="s-a8ffceff6c"></a>`tags`: `["collections"]`
- <a id="s-c19a5bba0d"></a>`x-riverhog-interface`: `"client-only-primitive"`
- <a id="s-4cda327f5a"></a>`x-riverhog-permission-requirements`: `[{"any_of":["collections:create"]}]`

### <a id="s-c462782213"></a>Request body

- `required`: `true`

| Media type | Schema |
|---|---|
| application/json | [CreateOrResumeCollectionUploadSessionRequest](../http-schemas/schemas-createorresumecollectionuploadsessionrequest.md) |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-3094ef177a"></a>`200` | Successful Response | application/json | [CreateOrResumeCollectionUploadSessionOut](../http-schemas/schemas-createorresumecollectionuploadsessionout.md) | not declared |
| <a id="s-7747b101f7"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-5aabb4476d"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-f6e118473b"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-729c665c42"></a>`409` | Conflict | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `conflict` |
| <a id="s-e72adfdcf0"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)
- [riverhog_client.ApiClient.create_or_resume_collection_upload_session](../../riverhog-client/python/riverhog-client-apiclient-create-or-resume-collection-upload-session.md)

### Referenced contract elements

- [schemas: CreateOrResumeCollectionUploadSessionOut](../http-schemas/schemas-createorresumecollectionuploadsessionout.md)
- [schemas: CreateOrResumeCollectionUploadSessionRequest](../http-schemas/schemas-createorresumecollectionuploadsessionrequest.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-eab6a32705"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/collections.py::create\_or\_resume\_collection\_upload\_session](../../../../../../riverhog/src/riverhog_api/routers/collections.py#L246)

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
        "line": 2179,
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
      "public_identity": "riverhog_client.ApiClient.create_or_resume_collection_upload_session",
      "source": {
        "line": 1107,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.create_or_resume_collection_upload_session"
      }
    }
  ],
  "method": "POST",
  "operation_id": "create_or_resume_collection_upload_session",
  "path": "/v1/collection-upload-sessions",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-upload-sessions/post`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5a79f7f613f17b679d5f0a6e2a80b8a76ac5d4e18d28961712e5bafd1a5c115f -->

```json
{
  "operationId": "create_or_resume_collection_upload_session",
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/CreateOrResumeCollectionUploadSessionRequest"
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
            "$ref": "#/components/schemas/CreateOrResumeCollectionUploadSessionOut"
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
  "summary": "Create Or Resume Collection Upload Session",
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
