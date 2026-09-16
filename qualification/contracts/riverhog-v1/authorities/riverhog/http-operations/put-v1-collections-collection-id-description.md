# PUT /v1/collections/{collection_id}/description

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:put-v1-collections-collection-id-description:173918242b -->

Replace Collection Description

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-af8c734346"></a>
- <a id="s-a7f7417487"></a>`operationId`: `"replace_collection_description"`
- <a id="s-4d7049a72b"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-88ac2f2821"></a>`summary`: `"Replace Collection Description"`
- <a id="s-5f8c7a6d5b"></a>`tags`: `["collections"]`
- <a id="s-67dee08ca7"></a>`x-riverhog-permission-requirements`: `[{"any_of":["collection-descriptions:manage"]}]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-f298dfd6d7"></a>`collection_id` | path | yes | not declared | type="integer"; minimum=1; title="Collection Id" |
| <a id="s-1894750874"></a>`If-Match` | header | yes | not declared | type="string"; pattern="^\"[0-9a-f]{64}\"$"; title="If-Match" |

### <a id="s-805a79ced6"></a>Request body

- `required`: `true`

| Media type | Schema |
|---|---|
| application/json | [ReplaceCollectionDescriptionRequest](../http-schemas/schemas-replacecollectiondescriptionrequest.md) |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-2c0b359078"></a>`200` | Successful Response | application/json | [CollectionDescriptionOut](../http-schemas/schemas-collectiondescriptionout.md) | not declared |
| <a id="s-29f5bd9918"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-9c4e75c826"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-05972b7342"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-7be43d128b"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [piggity collection describe](../../piggity/cli/piggity-collection-describe.md)
- [riverhog_client.ApiClient.replace_collection_description](../../riverhog-client/python/riverhog-client-apiclient-replace-collection-description.md)

### Referenced contract dossiers

- [schemas: CollectionDescriptionOut](../http-schemas/schemas-collectiondescriptionout.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: ReplaceCollectionDescriptionRequest](../http-schemas/schemas-replacecollectiondescriptionrequest.md)

## Governing policies

- <a id="pa-c1aa32afc4"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **OpenAPI authority:** [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9)
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`
- **Handler:** [riverhog/src/riverhog_api/routers/collections.py::replace_collection_description](../../../../../../riverhog/src/riverhog_api/routers/collections.py#L633)

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
      "command": "collection describe",
      "source": {
        "line": 2536,
        "module": "piggity.main",
        "path": "reference/riverhog/applications/piggity/src/piggity/main.py",
        "symbol": "collection_describe_cmd"
      }
    }
  ],
  "cli_commands": [
    "collection describe"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.replace_collection_description",
      "source": {
        "line": 1579,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.replace_collection_description"
      }
    }
  ],
  "method": "PUT",
  "operation_id": "replace_collection_description",
  "path": "/v1/collections/{collection_id}/description",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collections~1{collection_id}~1description/put`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: df86985a99a1a99fcec7f7726fc83f1387336c40182059c1c960c150c3cc0dd8 -->

```json
{
  "operationId": "replace_collection_description",
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
      "in": "header",
      "name": "If-Match",
      "required": true,
      "schema": {
        "pattern": "^\"[0-9a-f]{64}\"$",
        "title": "If-Match",
        "type": "string"
      }
    }
  ],
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/ReplaceCollectionDescriptionRequest"
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
            "$ref": "#/components/schemas/CollectionDescriptionOut"
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
  "summary": "Replace Collection Description",
  "tags": [
    "collections"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "collection-descriptions:manage"
      ]
    }
  ]
}
```

</details>
