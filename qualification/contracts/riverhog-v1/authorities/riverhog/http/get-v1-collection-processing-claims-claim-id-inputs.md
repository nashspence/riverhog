# GET /v1/collection-processing-claims/{claim_id}/inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:get-v1-collection-processing-claims-claim-id-inputs:9cd0e10c85 -->

List Processing Claim Inputs

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-dac556a071"></a>
- <a id="s-f39c7b747b"></a>`operationId`: list_processing_claim_inputs
- <a id="s-2ac824c2f3"></a>`summary`: List Processing Claim Inputs
- <a id="s-3e52af669d"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-96fa7aafb8"></a>`claim_id` | path | yes | type="string"; pattern="^[0-9a-f]{64}$" |
| <a id="s-eb344fd2cb"></a>`authority_sha256` | query | yes | type="string"; pattern="^[0-9a-f]{64}$" |
| <a id="s-fcfde1d0f3"></a>`start_ordinal` | query | no | type="integer"; minimum=0 |

### Responses

| Status | Description |
|---|---|
| <a id="s-fb73b7552e"></a>`200` | Successful Response |
| <a id="s-1755e5d034"></a>`400` | Bad Request |
| <a id="s-63d19755f1"></a>`401` | Unauthorized |
| <a id="s-6bb7563081"></a>`403` | Forbidden |
| <a id="s-6522fc4c00"></a>`500` | Internal Server Error |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"authority":"processing-claim-inputs","authority_parameter":"authority_sha256","cursor_parameter":"start_ordinal","fixed_limit":128,"kind":"exact-authority-page"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/collection-processing-claims/{claim_id}/inputs](#s-dac556a071) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-7a0d5ac555"></a>[parameter claim_id](#s-96fa7aafb8) | `length · characters · fixed` | shared above |
| <a id="s-06d022f4e9"></a>[parameter authority_sha256](#s-eb344fd2cb) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [Operation parity: list_processing_claim_inputs](../operation/operation-parity-list-processing-claim-inputs.md)

### Referenced contract dossiers

- [schemas: CollectionRootPageDocument](schemas-collectionrootpagedocument.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)

## Governing policies

- <a id="pa-fa13262366"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-db47edf428"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-f077d2f8b4"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-processing-claims~1{claim_id}~1inputs/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 89d237b43d2a0f6085c23afb7ecd7f068d4d3df637dbb8d1dc80fdb108c19887 -->

```json
{
  "operationId": "list_processing_claim_inputs",
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
            "$ref": "#/components/schemas/CollectionRootPageDocument"
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
  "summary": "List Processing Claim Inputs",
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
  ],
  "x-riverhog-read-collection": {
    "authority": "processing-claim-inputs",
    "authority_parameter": "authority_sha256",
    "cursor_parameter": "start_ordinal",
    "fixed_limit": 128,
    "kind": "exact-authority-page"
  }
}
```
