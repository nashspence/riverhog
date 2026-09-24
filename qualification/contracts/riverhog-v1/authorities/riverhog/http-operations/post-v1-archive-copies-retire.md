# POST /v1/archive/copies/retire

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:post-v1-archive-copies-retire:7a22d82ea7 -->

Retire Archive Copy

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-d742ef1582"></a>
- <a id="s-ace6734f77"></a>`operationId`: `"retire_archive_copy"`
- <a id="s-7d4d4809de"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-f1554f94a5"></a>`summary`: `"Retire Archive Copy"`
- <a id="s-63f95a931e"></a>`tags`: `["archive"]`
- <a id="s-8fee959f1d"></a>`x-riverhog-permission-requirements`: `[{"any_of":["archives:manage"]}]`

### <a id="s-c3063a6228"></a>Request body

- `required`: `true`

| Media type | Schema |
|---|---|
| application/json | [RetireArchiveCopyRequest](../http-schemas/schemas-retirearchivecopyrequest.md) |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-727ed61cf8"></a>`200` | Successful Response | application/json | [ArchiveCopyRetirementResultOut](../http-schemas/schemas-archivecopyretirementresultout.md) | not declared |
| <a id="s-1b8a617542"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-2b4e80c9ee"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-4f887813da"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-b4c84e0df8"></a>`404` | Not Found | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `not_found` |
| <a id="s-13d5ed3af7"></a>`409` | Conflict | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `conflict`, `invalid_state` |
| <a id="s-f26af1b3d1"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |
| <a id="s-fc07f61e9d"></a>`503` | Service Unavailable | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `service_unavailable` |

## Maintained corroboration

### Related interface records

- [a-riverhog-cli archive retire](../../a-riverhog-cli/cli/a-riverhog-cli-archive-retire.md)
- [riverhog_client.ApiClient.retire_archive_copy](../../riverhog-client/python/riverhog-client-apiclient-retire-archive-copy.md)

### Referenced contract elements

- [schemas: ArchiveCopyRetirementResultOut](../http-schemas/schemas-archivecopyretirementresultout.md)
- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)
- [schemas: RetireArchiveCopyRequest](../http-schemas/schemas-retirearchivecopyrequest.md)

## Governing policies

- <a id="pa-c8b0cdaa52"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/archive.py::retire\_archive\_copy](../../../../../../riverhog/src/riverhog_api/routers/archive.py#L176)

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
      "command": "archive retire",
      "executable": "a-riverhog-cli",
      "result_identity": "a-riverhog-cli-result/archive/retire/v1",
      "source": {
        "line": 3180,
        "module": "a_riverhog_cli.main",
        "path": "some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py",
        "symbol": "archive_retire_cmd"
      }
    }
  ],
  "cli_commands": [
    "archive retire"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.retire_archive_copy",
      "source": {
        "line": 2548,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.retire_archive_copy"
      }
    }
  ],
  "method": "POST",
  "operation_id": "retire_archive_copy",
  "path": "/v1/archive/copies/retire",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1archive~1copies~1retire/post`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 59cf6db3764587964aba51e6cef622d1575b6d9a5bcf7203117ce9c0ad762926 -->

```json
{
  "operationId": "retire_archive_copy",
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/RetireArchiveCopyRequest"
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
            "$ref": "#/components/schemas/ArchiveCopyRetirementResultOut"
          }
        }
      },
      "description": "Successful Response"
    },
    "400": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorOut"
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
            "$ref": "#/components/schemas/ErrorOut"
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
            "$ref": "#/components/schemas/ErrorOut"
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
            "$ref": "#/components/schemas/ErrorOut"
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
            "$ref": "#/components/schemas/ErrorOut"
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
            "$ref": "#/components/schemas/ErrorOut"
          }
        }
      },
      "description": "Internal Server Error",
      "x-riverhog-error-codes": [
        "internal_error"
      ]
    },
    "503": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorOut"
          }
        }
      },
      "description": "Service Unavailable",
      "x-riverhog-error-codes": [
        "service_unavailable"
      ]
    }
  },
  "security": [
    {
      "HTTPBearer": []
    }
  ],
  "summary": "Retire Archive Copy",
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
