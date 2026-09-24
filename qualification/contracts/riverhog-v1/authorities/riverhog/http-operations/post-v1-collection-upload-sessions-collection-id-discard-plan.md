# POST /v1/collection-upload-sessions/{collection_id}/discard-plan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:post-v1-collection-upload-sessions-collec-793cc2515d:91011b9184 -->

Plan Collection Upload Discard

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-f9960d3305"></a>
- <a id="s-267825dd11"></a>`operationId`: `"plan_collection_upload_discard"`
- <a id="s-bbbfb609cd"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-1db267fc9f"></a>`summary`: `"Plan Collection Upload Discard"`
- <a id="s-86906c68c4"></a>`tags`: `["collections"]`
- <a id="s-384dc1d7c4"></a>`x-riverhog-permission-requirements`: `[{"any_of":["collections:delete"]}]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-d31d70c952"></a>`collection_id` | path | yes | not declared | allOf=[(type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])"); (not=(const="0"))]; title="Collection Id" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-ee09029c45"></a>`200` | Successful Response | application/json | [CollectionUploadDiscardPlanOut](../http-schemas/schemas-collectionuploaddiscardplanout.md) | not declared |
| <a id="s-fad7a886d0"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-7ef61a079d"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-268bf9024a"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-d784170a16"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [a-riverhog-cli collection upload discard](../../a-riverhog-cli/cli/a-riverhog-cli-collection-upload-discard.md)
- [riverhog_client.ApiClient.plan_collection_upload_discard](../../riverhog-client/python/riverhog-client-apiclient-plan-collection-upload-discard.md)

### Referenced contract elements

- [schemas: CollectionUploadDiscardPlanOut](../http-schemas/schemas-collectionuploaddiscardplanout.md)
- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)

## Governing policies

- <a id="pa-f4dc40c0c2"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/collections.py::plan\_collection\_upload\_discard](../../../../../../riverhog/src/riverhog_api/routers/collections.py#L515)

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
      "command": "collection upload discard",
      "executable": "a-riverhog-cli",
      "result_identity": "a-riverhog-cli-result/collection/upload/discard/v1",
      "source": {
        "line": 2463,
        "module": "a_riverhog_cli.main",
        "path": "some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py",
        "symbol": "upload_discard_cmd"
      }
    }
  ],
  "cli_commands": [
    "collection upload discard"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.plan_collection_upload_discard",
      "source": {
        "line": 1484,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.plan_collection_upload_discard"
      }
    }
  ],
  "method": "POST",
  "operation_id": "plan_collection_upload_discard",
  "path": "/v1/collection-upload-sessions/{collection_id}/discard-plan",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-upload-sessions~1{collection_id}~1discard-plan/post`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5f230616fa94cfd3f3e46113f0055b7527017111b8ac5a230d51901db4d9c6dc -->

```json
{
  "operationId": "plan_collection_upload_discard",
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
            "$ref": "#/components/schemas/CollectionUploadDiscardPlanOut"
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
  "summary": "Plan Collection Upload Discard",
  "tags": [
    "collections"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "collections:delete"
      ]
    }
  ]
}
```

</details>
