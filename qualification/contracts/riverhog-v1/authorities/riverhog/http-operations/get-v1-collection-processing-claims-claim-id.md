# GET /v1/collection-processing-claims/{claim_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-collection-processing-claims-claim-id:517d0096bf -->

Get Processing Claim

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-2bc1733fc5"></a>
- <a id="s-2981e1f0af"></a>`operationId`: `"get_processing_claim"`
- <a id="s-b937c1104a"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-527801a03a"></a>`summary`: `"Get Processing Claim"`
- <a id="s-6103fe2c0d"></a>`tags`: `["collection-workflows"]`
- <a id="s-5bb9093034"></a>`x-riverhog-interface`: `"client-only-primitive"`
- <a id="s-adbeda7ed8"></a>`x-riverhog-permission-requirements`: `[{"any_of":["collection-transforms:control","collection-transforms:execute"]}]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-7d2524d322"></a>`claim_id` | path | yes | not declared | type="string"; pattern="^[0-9a-f]{64}$"; title="Claim Id" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-62aac1b9b4"></a>`200` | Successful Response | application/json | [ProcessingClaimDocument](../http-schemas/schemas-processingclaimdocument.md) | not declared |
| <a id="s-69f516d38d"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-029178300b"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-54de868e9e"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-ddcd97dc11"></a>`404` | Not Found | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `not_found` |
| <a id="s-e77c19bd31"></a>`409` | Conflict | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `invalid_state` |
| <a id="s-db13ea2e29"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-ba3802496a"></a>[parameter claim_id](#s-7d2524d322) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient.get_processing_claim](../../riverhog-client/python/riverhog-client-apiclient-get-processing-claim.md)

### Referenced contract elements

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: ProcessingClaimDocument](../http-schemas/schemas-processingclaimdocument.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-04a901932b"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-ec54db9c29"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/workflows.py::get\_processing\_claim](../../../../../../riverhog/src/riverhog_api/routers/workflows.py#L284)

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
      "public_identity": "riverhog_client.ApiClient.get_processing_claim",
      "source": {
        "line": 228,
        "module": "riverhog_client.workflows",
        "path": "packages/riverhog-client/src/riverhog_client/workflows.py",
        "symbol": "CollectionWorkflowMethods.get_processing_claim"
      }
    }
  ],
  "method": "GET",
  "operation_id": "get_processing_claim",
  "path": "/v1/collection-processing-claims/{claim_id}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-processing-claims~1{claim_id}/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3e793d5b2e09aaeeb4eea1b90f646d8ada23da736944b3016075fc21bea4675a -->

```json
{
  "operationId": "get_processing_claim",
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
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ProcessingClaimDocument"
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
  "summary": "Get Processing Claim",
  "tags": [
    "collection-workflows"
  ],
  "x-riverhog-interface": "client-only-primitive",
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "collection-transforms:control",
        "collection-transforms:execute"
      ]
    }
  ]
}
```

</details>
