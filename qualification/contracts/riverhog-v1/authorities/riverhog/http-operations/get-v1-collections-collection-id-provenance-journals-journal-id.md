# GET /v1/collections/{collection_id}/provenance/journals/{journal_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-collections-collection-id-provenan-3a9ce32406:3d397774e9 -->

Stream Collection Provenance Journal

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-134aff881b"></a>
- <a id="s-4c94154a95"></a>`operationId`: `"stream_collection_provenance_journal"`
- <a id="s-87943351ef"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-c4c0625828"></a>`summary`: `"Stream Collection Provenance Journal"`
- <a id="s-5a9d34678d"></a>`tags`: `["provenance"]`
- <a id="s-4d895a8080"></a>`x-riverhog-interface`: `"client-only-primitive"`
- <a id="s-8fbbfff303"></a>`x-riverhog-permission-requirements`: `[{"any_of":["provenance:export"]}]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-a4ef2bea49"></a>`collection_id` | path | yes | not declared | allOf=[(type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])"); (not=(const="0"))]; title="Collection Id" |
| <a id="s-c5835bbcc5"></a>`journal_id` | path | yes | not declared | type="string"; pattern="^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"; title="Journal Id" |
| <a id="s-4fba5c2893"></a>`Range` | header | no | not declared | anyOf=[(type="string"); (type="null")]; title="Range" |
| <a id="s-c8fe3179f9"></a>`If-Match` | header | no | not declared | anyOf=[(type="string"); (type="null")]; title="If-Match" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-92f2a107ee"></a>`200` | Exact immutable provenance journal. | application/json-seq | type="string"; format="binary" | not declared |
| <a id="s-97663fb9b7"></a>`206` | Exact immutable provenance journal byte range. | application/json-seq | type="string"; format="binary" | not declared |
| <a id="s-92337c450c"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-d3af899133"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-a469d6dc90"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-31ed9a3535"></a>`404` | Not Found | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `not_found` |
| <a id="s-b74cf514a8"></a>`412` | Precondition Failed | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `precondition_failed` |
| <a id="s-2fff5173f2"></a>`428` | Precondition Required | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `precondition_required` |
| <a id="s-2339147e1d"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

#### Response headers

| Status | Header | Required | Schema | Description |
|---|---|---|---|---|
| `200` | <a id="s-659a7f71f5"></a>`Accept-Ranges` | yes | type="string"; const="bytes" |  |
| `200` | <a id="s-0f06d80d5e"></a>`Content-Length` | yes | type="integer"; minimum=0 | Exact response-body length in bytes. |
| `200` | <a id="s-3374d97df8"></a>`ETag` | yes | type="string"; pattern="^\"[0-9a-f]{64}\"$" | Quoted SHA-256 identity of the journal bytes. |
| `206` | <a id="s-e11e9623c4"></a>`Accept-Ranges` | yes | type="string"; const="bytes" |  |
| `206` | <a id="s-27c91048f3"></a>`Content-Length` | yes | type="integer" |  |
| `206` | <a id="s-7c6a669207"></a>`Content-Range` | yes | type="string" |  |
| `206` | <a id="s-dbb4efa7e0"></a>`ETag` | yes | type="string" |  |

## Maintained corroboration

### Related interface records

- [a-riverhog-cli collection provenance export](../../a-riverhog-cli/cli/a-riverhog-cli-collection-provenance-export.md)
- [riverhog_client.ApiClient.stream_collection_provenance_journal](../../riverhog-client/python/riverhog-client-apiclient-stream-collection-provenance-journal.md)

### Referenced contract elements

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-8caf35e213"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/provenance.py::stream\_collection\_provenance\_journal](../../../../../../riverhog/src/riverhog_api/routers/provenance.py#L174)

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
      "command": "collection provenance export",
      "executable": "a-riverhog-cli",
      "result_identity": "a-riverhog-cli-result/collection/provenance/export/v1",
      "source": {
        "line": 2690,
        "module": "a_riverhog_cli.main",
        "path": "some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py",
        "symbol": "provenance_export_cmd"
      }
    }
  ],
  "cli_commands": [
    "collection provenance export"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.stream_collection_provenance_journal",
      "source": {
        "line": 1694,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.stream_collection_provenance_journal"
      }
    }
  ],
  "method": "GET",
  "operation_id": "stream_collection_provenance_journal",
  "path": "/v1/collections/{collection_id}/provenance/journals/{journal_id}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "stream-or-empty"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collections~1{collection_id}~1provenance~1journals~1{journal_id}/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 13549ab99cfde82af8637a856a56848cb3699b629c2a70e2020de5a6e7f2ad21 -->

```json
{
  "operationId": "stream_collection_provenance_journal",
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

</details>
