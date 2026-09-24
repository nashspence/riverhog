# POST /v1/collection-processing-claims/{claim_id}/capabilities

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:post-v1-collection-processing-claims-clai-dfec4c2ebf:fcf08c4438 -->

Create Processing Capability

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-14e8cf6a72"></a>
- <a id="s-7b539cd145"></a>`operationId`: `"create_processing_capability"`
- <a id="s-90b4519db2"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-33f526a015"></a>`summary`: `"Create Processing Capability"`
- <a id="s-c708c0b93d"></a>`tags`: `["collection-workflows"]`
- <a id="s-1b4d56b8de"></a>`x-riverhog-interface`: `"client-only-primitive"`
- <a id="s-a5cde85b2f"></a>`x-riverhog-permission-requirements`: `[{"any_of":["collection-processing:execute"]}]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-f5e6c98ff5"></a>`claim_id` | path | yes | not declared | type="string"; pattern="^[0-9a-f]{64}$"; title="Claim Id" |

### <a id="s-00b1d345ce"></a>Request body

- `required`: `true`

| Media type | Schema |
|---|---|
| application/json | [ProcessingCapabilityCreateDocument](../http-schemas/schemas-processingcapabilitycreatedocument.md) |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-3f274725a6"></a>`200` | Successful Response | application/json | [ProcessingCapabilityDocument](../http-schemas/schemas-processingcapabilitydocument.md) | not declared |
| <a id="s-59c01857ab"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-27b74b90dd"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-77e87db659"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-761d51a50f"></a>`404` | Not Found | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `not_found` |
| <a id="s-271ac1e521"></a>`409` | Conflict | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `conflict`, `invalid_state` |
| <a id="s-7df5274677"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-2a0921559c"></a>[parameter claim_id](#s-f5e6c98ff5) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient.create_processing_capability](../../riverhog-client/python/riverhog-client-apiclient-create-processing-capability.md)

### Referenced contract elements

- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)
- [schemas: ProcessingCapabilityCreateDocument](../http-schemas/schemas-processingcapabilitycreatedocument.md)
- [schemas: ProcessingCapabilityDocument](../http-schemas/schemas-processingcapabilitydocument.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-745575ad67"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-92def7f9d8"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/workflows.py::create\_processing\_capability](../../../../../../riverhog/src/riverhog_api/routers/workflows.py#L389)

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
      "public_identity": "riverhog_client.ApiClient.create_processing_capability",
      "source": {
        "line": 399,
        "module": "riverhog_client.workflows",
        "path": "packages/riverhog-client/src/riverhog_client/workflows.py",
        "symbol": "CollectionWorkflowMethods.create_processing_capability"
      }
    }
  ],
  "method": "POST",
  "operation_id": "create_processing_capability",
  "path": "/v1/collection-processing-claims/{claim_id}/capabilities",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-processing-claims~1{claim_id}~1capabilities/post`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4229b78f662e21a6761d17cc265b9ec64fb1ecf09030137ffaabdd56ac8d6741 -->

```json
{
  "operationId": "create_processing_capability",
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
          "$ref": "#/components/schemas/ProcessingCapabilityCreateDocument"
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
            "$ref": "#/components/schemas/ProcessingCapabilityDocument"
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
        "conflict",
        "invalid_state"
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
  "summary": "Create Processing Capability",
  "tags": [
    "collection-workflows"
  ],
  "x-riverhog-interface": "client-only-primitive",
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "collection-processing:execute"
      ]
    }
  ]
}
```

</details>
