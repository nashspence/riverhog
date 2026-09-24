# GET /v1/archive/upload-copy-intents/{collection_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-archive-upload-copy-intents-collection-id:d51d2b9e12 -->

Get Upload Copy Intents

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-dfba47f1bb"></a>
- <a id="s-fc767d19a6"></a>`operationId`: `"get_upload_copy_intents"`
- <a id="s-f820490cbe"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-6095d1a6f8"></a>`summary`: `"Get Upload Copy Intents"`
- <a id="s-56d3690e7c"></a>`tags`: `["archive"]`
- <a id="s-80dea6ae8c"></a>`x-riverhog-permission-requirements`: `[{"any_of":["archives:manage"]}]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-e0897a131a"></a>`collection_id` | path | yes | not declared | allOf=[(type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])"); (not=(const="0"))]; title="Collection Id" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-584ad596fd"></a>`200` | Successful Response | application/json | [UploadCopyIntentsOut](../http-schemas/schemas-uploadcopyintentsout.md) | not declared |
| <a id="s-4e20268ec2"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-30070be20a"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-ef14cb7f17"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-1a88cf46ad"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [a-riverhog-cli archive copy-job intents](../../a-riverhog-cli/cli/a-riverhog-cli-archive-copy-job-intents.md)
- [riverhog_client.ApiClient.get_upload_copy_intents](../../riverhog-client/python/riverhog-client-apiclient-get-upload-copy-intents.md)

### Referenced contract elements

- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)
- [schemas: UploadCopyIntentsOut](../http-schemas/schemas-uploadcopyintentsout.md)

## Governing policies

- <a id="pa-99ebd70300"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/archive.py::get\_upload\_copy\_intents](../../../../../../riverhog/src/riverhog_api/routers/archive.py#L43)

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
      "command": "archive copy-job intents",
      "executable": "a-riverhog-cli",
      "result_identity": "a-riverhog-cli-result/archive/copy-job/intents/v1",
      "source": {
        "line": 3036,
        "module": "a_riverhog_cli.main",
        "path": "some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py",
        "symbol": "archive_copy_intents_cmd"
      }
    }
  ],
  "cli_commands": [
    "archive copy-job intents"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.get_upload_copy_intents",
      "source": {
        "line": 2469,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.get_upload_copy_intents"
      }
    }
  ],
  "method": "GET",
  "operation_id": "get_upload_copy_intents",
  "path": "/v1/archive/upload-copy-intents/{collection_id}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1archive~1upload-copy-intents~1{collection_id}/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 15abb8c34862be843d327b4e13b25a93a0739b89382fd6a95a8f9b745b48265f -->

```json
{
  "operationId": "get_upload_copy_intents",
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
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/UploadCopyIntentsOut"
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
    }
  },
  "security": [
    {
      "HTTPBearer": []
    }
  ],
  "summary": "Get Upload Copy Intents",
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
