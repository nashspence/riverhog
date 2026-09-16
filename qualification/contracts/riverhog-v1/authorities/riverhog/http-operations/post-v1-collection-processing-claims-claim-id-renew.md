# POST /v1/collection-processing-claims/{claim_id}/renew

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:post-v1-collection-processing-claims-claim-id-renew:0794f0367f -->

Renew Processing Claim

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-d31deddf82"></a>
- <a id="s-b183cbd7fc"></a>`operationId`: `"renew_processing_claim"`
- <a id="s-ea7943f1c1"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-c1337a9b23"></a>`summary`: `"Renew Processing Claim"`
- <a id="s-9c58bb1be5"></a>`tags`: `["collection-workflows"]`
- <a id="s-d8bc2906f5"></a>`x-riverhog-interface`: `"client-only-primitive"`
- <a id="s-a144c3ea01"></a>`x-riverhog-permission-requirements`: `[{"any_of":["collection-transforms:control","collection-transforms:execute"]}]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-84b65d13d3"></a>`claim_id` | path | yes | not declared | type="string"; pattern="^[0-9a-f]{64}$"; title="Claim Id" |

### <a id="s-d10260b036"></a>Request body

- `required`: `true`

| Media type | Schema |
|---|---|
| application/json | [ProcessingClaimRenewDocument](../http-schemas/schemas-processingclaimrenewdocument.md) |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-b7cb7edcbc"></a>`200` | Successful Response | application/json | [ProcessingClaimDocument](../http-schemas/schemas-processingclaimdocument.md) | not declared |
| <a id="s-282f881262"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-f2e4145a82"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-411b8d95af"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-ceb17b6f69"></a>`404` | Not Found | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `not_found` |
| <a id="s-86a8e74460"></a>`409` | Conflict | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `conflict`, `invalid_state` |
| <a id="s-0563cee0e6"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-b3df110144"></a>[parameter claim_id](#s-84b65d13d3) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient.renew_processing_claim](../../riverhog-client/python/riverhog-client-apiclient-renew-processing-claim.md)

### Referenced contract dossiers

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: ProcessingClaimDocument](../http-schemas/schemas-processingclaimdocument.md)
- [schemas: ProcessingClaimRenewDocument](../http-schemas/schemas-processingclaimrenewdocument.md)

## Governing policies

- <a id="pa-cdb11f45e5"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-17e833ed46"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **OpenAPI authority:** [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9)
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`
- **Handler:** [riverhog/src/riverhog_api/routers/workflows.py::renew_processing_claim](../../../../../../riverhog/src/riverhog_api/routers/workflows.py#L298)

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
      "public_identity": "riverhog_client.ApiClient.renew_processing_claim",
      "source": {
        "line": 253,
        "module": "riverhog_client.workflows",
        "path": "packages/riverhog-client/src/riverhog_client/workflows.py",
        "symbol": "CollectionWorkflowMethods.renew_processing_claim"
      }
    }
  ],
  "method": "POST",
  "operation_id": "renew_processing_claim",
  "path": "/v1/collection-processing-claims/{claim_id}/renew",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-processing-claims~1{claim_id}~1renew/post`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d9e21cabf3030d901480d7b5491d7dc5d695988b36614535234d9f1f42535e9b -->

```json
{
  "operationId": "renew_processing_claim",
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
  "requestBody": {
    "content": {
      "application/json": {
        "schema": {
          "$ref": "#/components/schemas/ProcessingClaimRenewDocument"
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
        "conflict",
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
  "summary": "Renew Processing Claim",
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
