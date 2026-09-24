# GET /v1/collection-processing-claims/{claim_id}/plan/artifacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-collection-processing-claims-claim-b0a852bb3e:182adc4756 -->

List Processing Claim Artifacts

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-cf894df1c2"></a>
- <a id="s-00f49e2ae8"></a>`operationId`: `"list_processing_claim_artifacts"`
- <a id="s-040d3e9296"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-22aad46c09"></a>`summary`: `"List Processing Claim Artifacts"`
- <a id="s-27225c0490"></a>`tags`: `["collection-workflows"]`
- <a id="s-86eb8dada3"></a>`x-riverhog-interface`: `"client-only-primitive"`
- <a id="s-c1a802c593"></a>`x-riverhog-permission-requirements`: `[{"any_of":["collection-processing:control","collection-processing:execute"]}]`
- <a id="s-661bc6a412"></a>`x-riverhog-read-collection`: `{"authority":"processing-claim-artifacts","authority_parameter":"identity_sha256","cursor_parameter":"start_ordinal","fixed_limit":128,"kind":"exact-authority-page"}`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-54928f2c7e"></a>`claim_id` | path | yes | not declared | type="string"; pattern="^[0-9a-f]{64}$"; title="Claim Id" |
| <a id="s-ea94e7774c"></a>`identity_sha256` | query | yes | not declared | type="string"; pattern="^[0-9a-f]{64}$"; title="Identity Sha256" |
| <a id="s-6c3ca564c5"></a>`start_ordinal` | query | no | `0` | type="integer"; minimum=0; title="Start Ordinal" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-8932c9dd97"></a>`200` | Successful Response | application/json | [CollectionArtifactPageDocument](../http-schemas/schemas-collectionartifactpagedocument.md) | not declared |
| <a id="s-e3d213d87d"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-d978d9353c"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-16c5ac3832"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-f36ed61c50"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: progression={"authority":"processing-claim-artifacts","authority_parameter":"identity_sha256","cursor_parameter":"start_ordinal","fixed_limit":128,"kind":"exact-authority-page"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/collection-processing-claims/{claim_id}/plan/artifacts](#s-cf894df1c2) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-e07bfa9fd0"></a>[parameter claim_id](#s-54928f2c7e) | `length · characters · fixed` | shared above |
| <a id="s-86acefe408"></a>[parameter identity_sha256](#s-ea94e7774c) | `length · characters · fixed` | shared above |

### Evidence gaps

The named contract groups have recorded evidence gaps in the following guarantees. Each group's page identifies its exact open guarantees and candidate tests:

- Each step stays within its declared limits.
- Continuing the work makes progress toward its declared completion.
- The operation works across multiple pages or chunks.
- Required data or work is not silently left out.
- Work can resume after a restart as its contract requires.

These guarantees let large tasks proceed in smaller steps: a limit on one page or chunk must not become a hidden limit on the whole task. Returning a first page correctly does not establish that continuation or recovery works. Capacity may explicitly reject, defer, or throttle work; it must not silently omit work.

Existing tests may establish individual cases. The gaps retain their recorded group-wide scope and do not establish a bug in every linked contract. Completion follows each contract's rules; mutable browsing carries no implied snapshot guarantee.

Required by: [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb).

Exact evidence groups for this contract element:

- [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md)

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient.list_processing_claim_artifacts](../../riverhog-client/python/riverhog-client-apiclient-list-processing-claim-artifacts.md)

### Referenced contract elements

- [schemas: CollectionArtifactPageDocument](../http-schemas/schemas-collectionartifactpagedocument.md)
- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-4fabd89fb6"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-e784e432ec"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)
- <a id="pa-3f2db3e120"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/workflows.py::list\_processing\_claim\_artifacts](../../../../../../riverhog/src/riverhog_api/routers/workflows.py#L211)

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
      "public_identity": "riverhog_client.ApiClient.list_processing_claim_artifacts",
      "source": {
        "line": 380,
        "module": "riverhog_client.workflows",
        "path": "packages/riverhog-client/src/riverhog_client/workflows.py",
        "symbol": "CollectionWorkflowMethods.list_processing_claim_artifacts"
      }
    }
  ],
  "method": "GET",
  "operation_id": "list_processing_claim_artifacts",
  "path": "/v1/collection-processing-claims/{claim_id}/plan/artifacts",
  "provider_evidence": null,
  "read_collection": {
    "authority": "processing-claim-artifacts",
    "authority_parameter": "identity_sha256",
    "cursor_parameter": "start_ordinal",
    "fixed_limit": 128,
    "kind": "exact-authority-page"
  },
  "response_authority": "canonical-document"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-processing-claims~1{claim_id}~1plan~1artifacts/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: de430d145771d74faa1a5cd39c4643e13ca92880261239e9ea95fb225b8e3448 -->

```json
{
  "operationId": "list_processing_claim_artifacts",
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
      "name": "identity_sha256",
      "required": true,
      "schema": {
        "pattern": "^[0-9a-f]{64}$",
        "title": "Identity Sha256",
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
            "$ref": "#/components/schemas/CollectionArtifactPageDocument"
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
  "summary": "List Processing Claim Artifacts",
  "tags": [
    "collection-workflows"
  ],
  "x-riverhog-interface": "client-only-primitive",
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "collection-processing:control",
        "collection-processing:execute"
      ]
    }
  ],
  "x-riverhog-read-collection": {
    "authority": "processing-claim-artifacts",
    "authority_parameter": "identity_sha256",
    "cursor_parameter": "start_ordinal",
    "fixed_limit": 128,
    "kind": "exact-authority-page"
  }
}
```

</details>
