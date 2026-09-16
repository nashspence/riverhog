# GET /v1/collections/{collection_id}/provenance/files/{path}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-collections-collection-id-provenan-6ee3284e33:8106c6a185 -->

Get Collection File Provenance

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-a206995255"></a>
- <a id="s-33fc368c81"></a>`operationId`: get_collection_file_provenance
- <a id="s-df90f8ba28"></a>`summary`: Get Collection File Provenance
- <a id="s-a7e603dbb4"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-163dce2876"></a>`collection_id` | path | yes | not declared | type="integer"; minimum=1 |
| <a id="s-24db11cead"></a>`path` | path | yes | not declared | type="string"; format="riverhog-canonical-relpath-v1"; minLength=1; maxLength=4096; pattern="^[^/\\\\]+(?:/[^/\\\\]+)*$"; allOf=additional keys=`not` \| additional keys=`not`; additional keys=`x-unicode-normalization` |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-15bacadd6f"></a>`200` | Successful Response | application/json | [CollectionFileProvenanceDetailOut](../http-schemas/schemas-collectionfileprovenancedetailout.md) | not declared |
| <a id="s-83bd1fa2e2"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-c6c3dc38ea"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-f09456aff1"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-86dbd7d5b7"></a>`404` | Not Found | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `not_found` |
| <a id="s-61e0e42d67"></a>`409` | Conflict | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `invalid_state` |
| <a id="s-161bb063af"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=4096; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-6aa8c138d1"></a>[parameter path](#s-24db11cead) | `length · characters · contract_max` | shared above |

## Maintained corroboration

### Related interface records

- [piggity collection provenance show](../../piggity/cli/piggity-collection-provenance-show.md)
- [riverhog_client.ApiClient.get_collection_file_provenance](../../riverhog-client/python/riverhog-client-apiclient-get-collection-file-provenance.md)

### Referenced contract dossiers

- [schemas: CollectionFileProvenanceDetailOut](../http-schemas/schemas-collectionfileprovenancedetailout.md)
- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)

## Governing policies

- <a id="pa-ddfca49053"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-f539eadd2a"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **OpenAPI authority:** [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9)
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`
- **Handler:** [riverhog/src/riverhog_api/routers/provenance.py::get_collection_file_provenance](../../../../../../riverhog/src/riverhog_api/routers/provenance.py#L122)

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
      "command": "collection provenance show",
      "source": {
        "line": 2623,
        "module": "piggity.main",
        "path": "reference/riverhog/applications/piggity/src/piggity/main.py",
        "symbol": "provenance_show_cmd"
      }
    }
  ],
  "cli_commands": [
    "collection provenance show"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.get_collection_file_provenance",
      "source": {
        "line": 1655,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.get_collection_file_provenance"
      }
    }
  ],
  "method": "GET",
  "operation_id": "get_collection_file_provenance",
  "path": "/v1/collections/{collection_id}/provenance/files/{path}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collections~1{collection_id}~1provenance~1files~1{path}/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2090cd52fbd665b4bf3a656622c4118a5405e783866ed5b2d7c05b11010b2870 -->

```json
{
  "operationId": "get_collection_file_provenance",
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
      "in": "path",
      "name": "path",
      "required": true,
      "schema": {
        "allOf": [
          {
            "not": {
              "pattern": "(?:^|/)\\.{1,2}(?:/|$)"
            }
          },
          {
            "not": {
              "pattern": "^\\s|\\s$"
            }
          }
        ],
        "format": "riverhog-canonical-relpath-v1",
        "maxLength": 4096,
        "minLength": 1,
        "pattern": "^[^/\\\\]+(?:/[^/\\\\]+)*$",
        "title": "Path",
        "type": "string",
        "x-unicode-normalization": "NFC"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/CollectionFileProvenanceDetailOut"
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
  "summary": "Get Collection File Provenance",
  "tags": [
    "provenance"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "provenance:read"
      ]
    }
  ]
}
```
