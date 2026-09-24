# GET /v1/artifact-selections/{selection_sha256}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:stove0:get-v1-artifact-selections-selection-sha256:699f9ded83 -->

Get Artifact Selection

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-c4ee91313e"></a>
- <a id="s-f26db81368"></a>`operationId`: `"get_artifact_selection"`
- <a id="s-19efa1c505"></a>`summary`: `"Get Artifact Selection"`
- <a id="s-66945d17b3"></a>`tags`: `["artifact-selections"]`
- <a id="s-2e1099aa34"></a>`x-riverhog-read-collection`: `{"authority":"artifact-selection","authority_parameter":"selection_sha256","cursor_parameter":"continuation","fixed_limit":256,"kind":"exact-authority-page"}`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-bef50296e9"></a>`selection_sha256` | path | yes | not declared | type="string"; title="Selection Sha256" |
| <a id="s-aa2e76ccfa"></a>`continuation` | query | no | not declared | anyOf=[(type="string"); (type="null")]; title="Continuation" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-3453778107"></a>`200` | Successful Response | application/json | [ArtifactSelectionPage](../http-schemas/schemas-artifactselectionpage.md) | not declared |
| <a id="s-c52052fdef"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-97b3183eb0"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-d266895c74"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-c2f146cba9"></a>`404` | Not Found | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `not_found` |
| <a id="s-1554018758"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: progression={"authority":"artifact-selection","authority_parameter":"selection_sha256","cursor_parameter":"continuation","fixed_limit":256,"kind":"exact-authority-page"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/artifact-selections/{selection_sha256}](#s-c4ee91313e) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

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

- [stove0-read-collection-progression/v1](../../../evidence/qualifications/stove0-read-collection-progression-v1/index.md)

## Maintained corroboration

### Related interface records

- [stove0 selection show](../../a-stove0-cli/cli/stove0-selection-show.md)
- [stove0_api_client.Stove0ApiClient.get_artifact_selection](../../stove0-api-client/python/stove0-api-client-stove0apiclient-get-artifact-selection.md)

### Referenced contract elements

- [schemas: ArtifactSelectionPage](../http-schemas/schemas-artifactselectionpage.md)
- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-45f23eb5ed"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-b00656c731"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [some-implementations/stove0/application/server/src/stove0\_api/app.py::create\_app.&lt;locals&gt;.get\_artifact\_selection](../../../../../../some-implementations/stove0/application/server/src/stove0_api/app.py#L752)

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

<details>
<summary>Exact structural binding record</summary>

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_bindings": [
    {
      "command": "selection show",
      "executable": "stove0",
      "result_identity": "stove0-cli-result/selection/show/v1",
      "source": {
        "line": 332,
        "module": "a_stove0_cli.main",
        "path": "some-implementations/stove0/application/client/src/a_stove0_cli/main.py",
        "symbol": "get_artifact_selection"
      }
    }
  ],
  "cli_commands": [
    "selection show"
  ],
  "client": "Stove0ApiClient",
  "client_bindings": [
    {
      "public_identity": "stove0_api_client.Stove0ApiClient.get_artifact_selection",
      "source": {
        "line": 294,
        "module": "stove0_api_client.client",
        "path": "some-implementations/stove0/packages/api-client/src/stove0_api_client/client.py",
        "symbol": "Stove0ApiClient.get_artifact_selection"
      }
    }
  ],
  "method": "GET",
  "operation_id": "get_artifact_selection",
  "path": "/v1/artifact-selections/{selection_sha256}",
  "provider_evidence": null,
  "read_collection": {
    "authority": "artifact-selection",
    "authority_parameter": "selection_sha256",
    "cursor_parameter": "continuation",
    "fixed_limit": 256,
    "kind": "exact-authority-page"
  },
  "response_authority": "canonical-document"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1artifact-selections~1{selection_sha256}/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 28ccfaeba8462c0abe952ef4b70fc1d46e617470548cc5c159e98677d5321a02 -->

```json
{
  "operationId": "get_artifact_selection",
  "parameters": [
    {
      "in": "path",
      "name": "selection_sha256",
      "required": true,
      "schema": {
        "title": "Selection Sha256",
        "type": "string"
      }
    },
    {
      "in": "query",
      "name": "continuation",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "type": "string"
          },
          {
            "type": "null"
          }
        ],
        "title": "Continuation"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ArtifactSelectionPage"
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
  "summary": "Get Artifact Selection",
  "tags": [
    "artifact-selections"
  ],
  "x-riverhog-read-collection": {
    "authority": "artifact-selection",
    "authority_parameter": "selection_sha256",
    "cursor_parameter": "continuation",
    "fixed_limit": 256,
    "kind": "exact-authority-page"
  }
}
```

</details>
