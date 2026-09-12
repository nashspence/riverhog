# GET /v1/artifact-selections/{selection_sha256}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:get-v1-artifact-selections-selection-sha256:277b0341e5 -->

Get Artifact Selection

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `artifact-selections` |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- `operationId`: get_artifact_selection
- `summary`: Get Artifact Selection

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| `selection_sha256` | path | yes | type="string" |
| `continuation` | query | no | anyOf=type="string" \| type="null" |

### Responses

| Status | Description |
|---|---|
| `200` | Successful Response |
| `400` | Bad Request |
| `401` | Unauthorized |
| `403` | Forbidden |
| `404` | Not Found |
| `500` | Internal Server Error |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| logical-result-cardinality | items | `segmented_no_total_max` | reason=bounded-route-progression |

## Maintained corroboration

### Related interface records

- [Operation parity: get_artifact_selection](../operation/operation-parity-get-artifact-selection.md)

### Referenced contract dossiers

- [schemas: ArtifactSelectionPage](schemas-artifactselectionpage.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/route-progression/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
