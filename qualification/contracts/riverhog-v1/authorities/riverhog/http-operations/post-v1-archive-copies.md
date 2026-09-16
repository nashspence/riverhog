# POST /v1/archive/copies

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:post-v1-archive-copies:eb648e2ac7 -->

Create Or Resume Archive Copy

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-8df4a71950"></a>
- <a id="s-fabf2aad91"></a>`operationId`: `"create_or_resume_archive_copy"`
- <a id="s-b14612bf43"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-ac95213ded"></a>`summary`: `"Create Or Resume Archive Copy"`
- <a id="s-81a792d037"></a>`tags`: `["archive"]`
- <a id="s-7ab47d8c6a"></a>`x-riverhog-permission-requirements`: `[{"any_of":["archives:manage"]}]`

### <a id="s-556eabb798"></a>Request body

- `required`: `true`

| Media type | Schema |
|---|---|
| application/json | [CreateArchiveCopyRequest](../http-schemas/schemas-createarchivecopyrequest.md) |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-39d8493e84"></a>`200` | Successful Response | application/json | [ArchiveCopyJobOut](../http-schemas/schemas-archivecopyjobout.md) | not declared |
| <a id="s-b7745610eb"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-ad6df6f94e"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-1ce05e5dcf"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-b417964ab7"></a>`404` | Not Found | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `not_found` |
| <a id="s-cc6d320143"></a>`409` | Conflict | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `conflict`, `invalid_state` |
| <a id="s-3a188b5f67"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [piggity archive copy start](../../piggity/cli/piggity-archive-copy-start.md)
- [riverhog_client.ApiClient.create_or_resume_archive_copy](../../riverhog-client/python/riverhog-client-apiclient-create-or-resume-archive-copy.md)

### Referenced contract dossiers

- [schemas: ArchiveCopyJobOut](../http-schemas/schemas-archivecopyjobout.md)
- [schemas: CreateArchiveCopyRequest](../http-schemas/schemas-createarchivecopyrequest.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-5590354b3b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **OpenAPI authority:** [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9)
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`
- **Handler:** [riverhog/src/riverhog_api/routers/archive.py::create_or_resume_archive_copy](../../../../../../riverhog/src/riverhog_api/routers/archive.py#L41)

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

<details>
<summary>Exact structural binding record</summary>

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_bindings": [
    {
      "command": "archive copy start",
      "source": {
        "line": 2969,
        "module": "piggity.main",
        "path": "reference/riverhog/applications/piggity/src/piggity/main.py",
        "symbol": "archive_copy_cmd"
      }
    }
  ],
  "cli_commands": [
    "archive copy start"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.create_or_resume_archive_copy",
      "source": {
        "line": 2405,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.create_or_resume_archive_copy"
      }
    }
  ],
  "method": "POST",
  "operation_id": "create_or_resume_archive_copy",
  "path": "/v1/archive/copies",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1archive~1copies/post`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d7580d6030a526c35862f4d9b363faba8b129bae0985fc4c53edc7fbd2fd06ee -->

```json
{
  "operationId": "create_or_resume_archive_copy",
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/CreateArchiveCopyRequest"
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
            "$ref": "#/components/schemas/ArchiveCopyJobOut"
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
  "summary": "Create Or Resume Archive Copy",
  "tags": [
    "archive"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "archives:manage"
      ]
    }
  ]
}
```

</details>
