# GET /v1/artifact-selections/{selection_sha256}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:get-v1-artifact-selections-selection-sha256:277b0341e5 -->

Get Artifact Selection

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [artifact-selections](families/artifact-selections/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-c4ee91313e"></a>
- <a id="s-f26db81368"></a>`operationId`: get_artifact_selection
- <a id="s-19efa1c505"></a>`summary`: Get Artifact Selection

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-bef50296e9"></a>`selection_sha256` | path | yes | type="string" |
| <a id="s-aa2e76ccfa"></a>`continuation` | query | no | anyOf=type="string" \| type="null" |

### Responses

| Status | Description |
|---|---|
| <a id="s-3453778107"></a>`200` | Successful Response |
| <a id="s-c52052fdef"></a>`400` | Bad Request |
| <a id="s-97b3183eb0"></a>`401` | Unauthorized |
| <a id="s-d266895c74"></a>`403` | Forbidden |
| <a id="s-c2f146cba9"></a>`404` | Not Found |
| <a id="s-1554018758"></a>`500` | Internal Server Error |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"authority":"artifact-selection","authority_parameter":"selection_sha256","cursor_parameter":"continuation","fixed_limit":256,"kind":"exact-authority-page"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/artifact-selections/{selection_sha256}](#s-c4ee91313e) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

## Maintained corroboration

### Related interface records

- [Operation parity: get_artifact_selection](../operation/operation-parity-get-artifact-selection.md)

### Referenced contract dossiers

- [schemas: ArtifactSelectionPage](schemas-artifactselectionpage.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)

## Governing policies

- <a id="pa-c8abc0237c"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-2250577ac5"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1artifact-selections~1{selection_sha256}/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c16c1450d2de53dd0091b71063fb9fd368e7cebbc183a834b07f252de6009801 -->

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
