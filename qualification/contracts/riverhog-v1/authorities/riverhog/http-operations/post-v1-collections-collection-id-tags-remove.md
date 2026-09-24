# POST /v1/collections/{collection_id}/tags:remove

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:post-v1-collections-collection-id-tags-remove:ef9502d670 -->

Remove Collection Tag

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-6c0b51a8a2"></a>
- <a id="s-4790c648e6"></a>`operationId`: `"remove_collection_tag"`
- <a id="s-2e2950b25e"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-8c37cb6586"></a>`summary`: `"Remove Collection Tag"`
- <a id="s-87b9e7c19d"></a>`tags`: `["collection-tags"]`
- <a id="s-0360883da6"></a>`x-riverhog-permission-requirements`: `[{"any_of":["collection-tags:manage"]}]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-db3db1097e"></a>`collection_id` | path | yes | not declared | allOf=[(type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])"); (not=(const="0"))]; title="Collection Id" |

### <a id="s-3fdde7ccbe"></a>Request body

- `required`: `true`

| Media type | Schema |
|---|---|
| application/json | [CollectionTagMutationRequest](../http-schemas/schemas-collectiontagmutationrequest.md) |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-ea545b124d"></a>`200` | Successful Response | application/json | [CollectionTagMutationOut](../http-schemas/schemas-collectiontagmutationout.md) | not declared |
| <a id="s-a1ead4ff81"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-ad23e293e1"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-6a5b3501b7"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-1aeecb146a"></a>`404` | Not Found | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `not_found` |
| <a id="s-edc265bca6"></a>`409` | Conflict | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `conflict` |
| <a id="s-40640ad73b"></a>`412` | Precondition Failed | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `precondition_failed` |
| <a id="s-6277d3c7be"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [a-riverhog-cli collection tag remove](../../a-riverhog-cli/cli/a-riverhog-cli-collection-tag-remove.md)
- [riverhog_client.ApiClient.remove_collection_tag](../../riverhog-client/python/riverhog-client-apiclient-remove-collection-tag.md)

### Referenced contract elements

- [schemas: CollectionTagMutationOut](../http-schemas/schemas-collectiontagmutationout.md)
- [schemas: CollectionTagMutationRequest](../http-schemas/schemas-collectiontagmutationrequest.md)
- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)

## Governing policies

- <a id="pa-c1855bfc7b"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/tags.py::remove\_collection\_tag](../../../../../../riverhog/src/riverhog_api/routers/tags.py#L163)

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
      "command": "collection tag remove",
      "executable": "a-riverhog-cli",
      "result_identity": "a-riverhog-cli-result/collection/tag/remove/v1",
      "source": {
        "line": 1088,
        "module": "a_riverhog_cli.main",
        "path": "some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py",
        "symbol": "collection_tag_remove_cmd"
      }
    }
  ],
  "cli_commands": [
    "collection tag remove"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.remove_collection_tag",
      "source": {
        "line": 2335,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.remove_collection_tag"
      }
    }
  ],
  "method": "POST",
  "operation_id": "remove_collection_tag",
  "path": "/v1/collections/{collection_id}/tags:remove",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collections~1{collection_id}~1tags:remove/post`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 188a12ff8def990b8e5b9fa89c906abfc540af1e34f6069e48fae2cb2abe4eb4 -->

```json
{
  "operationId": "remove_collection_tag",
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
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/CollectionTagMutationRequest"
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
            "$ref": "#/components/schemas/CollectionTagMutationOut"
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
        "conflict"
      ]
    },
    "412": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorOut"
          }
        }
      },
      "description": "Precondition Failed",
      "x-riverhog-error-codes": [
        "precondition_failed"
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
  "summary": "Remove Collection Tag",
  "tags": [
    "collection-tags"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "collection-tags:manage"
      ]
    }
  ]
}
```

</details>
