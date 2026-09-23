# PUT /v1/collection-upload-sessions/{collection_id}/volumes/{volume_id}/units/{unit}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:put-v1-collection-upload-sessions-collect-859266a156:2e5f66e1f0 -->

Put Collection Upload Session Unit

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-56fb5451d4"></a>
- <a id="s-59edab6dfe"></a>`operationId`: `"put_collection_upload_session_unit"`
- <a id="s-18fe034da0"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-ebcc0b06a2"></a>`summary`: `"Put Collection Upload Session Unit"`
- <a id="s-922b12e401"></a>`tags`: `["collections"]`
- <a id="s-3d419f4700"></a>`x-riverhog-interface`: `"client-only-primitive"`
- <a id="s-dbf46d8cec"></a>`x-riverhog-permission-requirements`: `[{"any_of":["collections:create"]}]`

### Parameters

| Name | In | Required | Default | Schema | Description |
|---|---|---:|---|---|---|
| <a id="s-b098192ee1"></a>`collection_id` | path | yes | not declared | allOf=[(type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])"); (not=(const="0"))]; title="Collection Id" |  |
| <a id="s-b77d4eaf73"></a>`volume_id` | path | yes | not declared | type="string"; pattern="^(?:pack\|segment)-[0-9a-f]{64}$"; title="Volume Id" |  |
| <a id="s-606a5c1e12"></a>`unit` | path | yes | not declared | type="string"; pattern="^(?:0\|[1-9][0-9]*)(?![\\s\\S])"; title="Unit" |  |
| <a id="s-8408816c84"></a>`If-Match` | header | yes | not declared | type="string"; pattern="^\"[0-9a-f]{64}\"$"; title="If-Match" |  |
| <a id="s-4b9aa015e5"></a>`Content-Length` | header | yes | not declared | type="integer"; minimum=0 | Exact request-body length in bytes. |

### <a id="s-2c66ce80f5"></a>Request body

- `required`: `true`

| Media type | Schema |
|---|---|
| application/octet-stream | type="string"; format="binary"; contentMediaType="application/octet-stream"; title="Content" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-6d38f9f84e"></a>`200` | Successful Response | application/json | [CollectionUploadUnitWorkDocument](../http-schemas/schemas-collectionuploadunitworkdocument.md) | not declared |
| <a id="s-2ccb8b683f"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-cc97e447eb"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-f79b60e8a2"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-0780afcadf"></a>`404` | Not Found | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `not_found` |
| <a id="s-ba5f434727"></a>`409` | Conflict | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `conflict` |
| <a id="s-2e46e1490b"></a>`411` | Length Required | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `length_required` |
| <a id="s-1216d6dc27"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [a-riverhog-cli collection upload start](../../a-riverhog-cli/cli/a-riverhog-cli-collection-upload-start.md)
- [riverhog_client.ApiClient.put_collection_upload_session_unit](../../riverhog-client/python/riverhog-client-apiclient-put-collection-upload-session-unit.md)

### Referenced contract elements

- [schemas: CollectionUploadUnitWorkDocument](../http-schemas/schemas-collectionuploadunitworkdocument.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-23f4517b56"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/collections.py::put\_collection\_upload\_session\_unit](../../../../../../riverhog/src/riverhog_api/routers/collections.py#L582)

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
      "executable": "a-riverhog-cli",
      "result_identity": "a-riverhog-cli-result/collection/upload/start/v1",
      "source": {
        "line": 2185,
        "module": "a_riverhog_cli.main",
        "path": "some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py",
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
      "public_identity": "riverhog_client.ApiClient.put_collection_upload_session_unit",
      "source": {
        "line": 1527,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.put_collection_upload_session_unit"
      }
    }
  ],
  "method": "PUT",
  "operation_id": "put_collection_upload_session_unit",
  "path": "/v1/collection-upload-sessions/{collection_id}/volumes/{volume_id}/units/{unit}",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "canonical-document"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-upload-sessions~1{collection_id}~1volumes~1{volume_id}~1units~1{unit}/put`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a8c52bdf0743cc1385fa03e99bdc1c04eb1ec42c3778e2a7d7d8b93d76e52b13 -->

```json
{
  "operationId": "put_collection_upload_session_unit",
  "parameters": [
    {
      "in": "path",
      "name": "collection_id",
      "required": true,
      "schema": {
        "allOf": [
          {
            "pattern": "^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[0-1][0-9]{17}|92[0-1][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[0-1][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6][0-9]{0}|9223372036854775807)(?![\\s\\S])",
            "type": "string"
          },
          {
            "not": {
              "const": "0"
            }
          }
        ],
        "title": "Collection Id"
      }
    },
    {
      "in": "path",
      "name": "volume_id",
      "required": true,
      "schema": {
        "pattern": "^(?:pack|segment)-[0-9a-f]{64}$",
        "title": "Volume Id",
        "type": "string"
      }
    },
    {
      "in": "path",
      "name": "unit",
      "required": true,
      "schema": {
        "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
        "title": "Unit",
        "type": "string"
      }
    },
    {
      "in": "header",
      "name": "If-Match",
      "required": true,
      "schema": {
        "pattern": "^\"[0-9a-f]{64}\"$",
        "title": "If-Match",
        "type": "string"
      }
    },
    {
      "description": "Exact request-body length in bytes.",
      "in": "header",
      "name": "Content-Length",
      "required": true,
      "schema": {
        "minimum": 0,
        "type": "integer"
      }
    }
  ],
  "requestBody": {
    "content": {
      "application/octet-stream": {
        "schema": {
          "contentMediaType": "application/octet-stream",
          "format": "binary",
          "title": "Content",
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
            "$ref": "#/components/schemas/CollectionUploadUnitWorkDocument"
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
  "summary": "Put Collection Upload Session Unit",
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
