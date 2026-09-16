# GET /v1/collection-processing-claims/{claim_id}/outcomes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-collection-processing-claims-claim-8b38dffa94:565ea757d4 -->

List Processing Claim Outcomes

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-46afca9083"></a>
- <a id="s-a3d86b9e90"></a>`operationId`: `"list_processing_claim_outcomes"`
- <a id="s-652df33de7"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-a588fae842"></a>`summary`: `"List Processing Claim Outcomes"`
- <a id="s-5cf61448ac"></a>`tags`: `["collection-workflows"]`
- <a id="s-ca85553076"></a>`x-riverhog-interface`: `"client-only-primitive"`
- <a id="s-0e79e80cde"></a>`x-riverhog-permission-requirements`: `[{"any_of":["collection-transforms:control"]}]`
- <a id="s-45fd234f1c"></a>`x-riverhog-read-collection`: `{"authority":"processing-claim-outcomes","authority_parameter":"authority_sha256","cursor_parameter":"start_ordinal","fixed_limit":128,"kind":"exact-authority-page"}`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-149540dba9"></a>`claim_id` | path | yes | not declared | type="string"; pattern="^[0-9a-f]{64}$"; title="Claim Id" |
| <a id="s-000a0f0754"></a>`authority_sha256` | query | yes | not declared | type="string"; pattern="^[0-9a-f]{64}$"; title="Authority Sha256" |
| <a id="s-86f7a2f33a"></a>`start_ordinal` | query | no | `0` | type="integer"; minimum=0; title="Start Ordinal" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-16288a2f24"></a>`200` | Successful Response | application/json | [ProcessingOutcomePageDocument](../http-schemas/schemas-processingoutcomepagedocument.md) | not declared |
| <a id="s-6c6ccbcf73"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-413489ab16"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-f150ebf170"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-41df1bfb19"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"authority":"processing-claim-outcomes","authority_parameter":"authority_sha256","cursor_parameter":"start_ordinal","fixed_limit":128,"kind":"exact-authority-page"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/collection-processing-claims/{claim_id}/outcomes](#s-46afca9083) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-00d7f4fdf3"></a>[parameter claim_id](#s-149540dba9) | `length · characters · fixed` | shared above |
| <a id="s-db85312016"></a>[parameter authority_sha256](#s-000a0f0754) | `length · characters · fixed` | shared above |

### Progression evidence and open obligations

These are candidate test bindings. Group-wide progression claims remain unestablished; inspect the test scopes before applying a result to this contract.

- [riverhog-read-collection-progression/v1](../../../evidence/sources.md#e-5707b3a2d3-1536c4a29a)

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient.list_processing_claim_outcomes](../../riverhog-client/python/riverhog-client-apiclient-list-processing-claim-outcomes.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: ProcessingOutcomePageDocument](../http-schemas/schemas-processingoutcomepagedocument.md)

## Governing policies

- <a id="pa-724e68d795"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-520e365f9e"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-bb05153a6c"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **OpenAPI authority:** [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9)
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`
- **Handler:** [riverhog/src/riverhog_api/routers/workflows.py::list_processing_claim_outcomes](../../../../../../riverhog/src/riverhog_api/routers/workflows.py#L653)

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

<details>
<summary>Exact structural binding record</summary>

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_bindings": [],
  "cli_commands": [],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.list_processing_claim_outcomes",
      "source": {
        "line": 627,
        "module": "riverhog_client.workflows",
        "path": "packages/riverhog-client/src/riverhog_client/workflows.py",
        "symbol": "CollectionWorkflowMethods.list_processing_claim_outcomes"
      }
    }
  ],
  "method": "GET",
  "operation_id": "list_processing_claim_outcomes",
  "path": "/v1/collection-processing-claims/{claim_id}/outcomes",
  "provider_evidence": null,
  "read_collection": {
    "authority": "processing-claim-outcomes",
    "authority_parameter": "authority_sha256",
    "cursor_parameter": "start_ordinal",
    "fixed_limit": 128,
    "kind": "exact-authority-page"
  },
  "response_authority": "canonical-document"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-processing-claims~1{claim_id}~1outcomes/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5dc162109e8f3660a195aa7ceaf34f029be4aaa4c1472e38366f48a68e5f12a3 -->

```json
{
  "operationId": "list_processing_claim_outcomes",
  "parameters": [
    {
      "in": "path",
      "name": "claim_id",
      "required": true,
      "schema": {
        "pattern": "^[0-9a-f]{64}$",
        "title": "Claim Id",
        "type": "string"
      }
    },
    {
      "in": "query",
      "name": "authority_sha256",
      "required": true,
      "schema": {
        "pattern": "^[0-9a-f]{64}$",
        "title": "Authority Sha256",
        "type": "string"
      }
    },
    {
      "in": "query",
      "name": "start_ordinal",
      "required": false,
      "schema": {
        "default": 0,
        "minimum": 0,
        "title": "Start Ordinal",
        "type": "integer"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ProcessingOutcomePageDocument"
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
  "summary": "List Processing Claim Outcomes",
  "tags": [
    "collection-workflows"
  ],
  "x-riverhog-interface": "client-only-primitive",
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "collection-transforms:control"
      ]
    }
  ],
  "x-riverhog-read-collection": {
    "authority": "processing-claim-outcomes",
    "authority_parameter": "authority_sha256",
    "cursor_parameter": "start_ordinal",
    "fixed_limit": 128,
    "kind": "exact-authority-page"
  }
}
```

</details>
