# GET /v1/collection-processing-claims/{claim_id}/derivation/output-edges

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:get-v1-collection-processing-claims-claim-36428c3cf2:d8efa8844e -->

List Processing Claim Disposition Outputs

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [collection-processing-claims](families/collection-processing-claims/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-828059adb9"></a>
- <a id="s-7cced43bea"></a>`operationId`: list_processing_claim_disposition_outputs
- <a id="s-1b9572b0ba"></a>`summary`: List Processing Claim Disposition Outputs
- <a id="s-eee7a651ad"></a>`security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| <a id="s-1f71dbde35"></a>`claim_id` | path | yes | type="string"; pattern="^[0-9a-f]{64}$" |
| <a id="s-487569e835"></a>`authority_sha256` | query | yes | type="string"; pattern="^[0-9a-f]{64}$" |
| <a id="s-ac085a0c36"></a>`start_ordinal` | query | no | type="integer"; minimum=0 |

### Responses

| Status | Description |
|---|---|
| <a id="s-c87cbf0ee0"></a>`200` | Successful Response |
| <a id="s-d322e2b1b2"></a>`400` | Bad Request |
| <a id="s-290faf8845"></a>`401` | Unauthorized |
| <a id="s-8b4bdfa02b"></a>`403` | Forbidden |
| <a id="s-774b1a99b1"></a>`404` | Not Found |
| <a id="s-1f9fd3f1b5"></a>`409` | Conflict |
| <a id="s-1abcc6f204"></a>`500` | Internal Server Error |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"authority":"processing-claim-disposition-outputs","authority_parameter":"authority_sha256","cursor_parameter":"start_ordinal","fixed_limit":128,"kind":"exact-authority-page"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/collection-processing-claims/{claim_id}/derivation/output-edges](#s-828059adb9) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-9fccb91cf3"></a>[parameter claim_id](#s-1f71dbde35) | `length · characters · fixed` | shared above |
| <a id="s-c9078cf0f3"></a>[parameter authority_sha256](#s-487569e835) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [Operation parity: list_processing_claim_disposition_outputs](../operation/operation-parity-list-processing-claim-disposition-outputs.md)

### Referenced contract dossiers

- [schemas: ArtifactDispositionOutputPageDocument](schemas-artifactdispositionoutputpagedocument.md)
- [schemas: ErrorResponse](schemas-errorresponse.md)

## Governing policies

- <a id="pa-ca4ded524b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-526f7bc4ac"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-4cbc2cc0b8"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-processing-claims~1{claim_id}~1derivation~1output-edges/get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 939a050c7dfcc4d9dbf6ca899258c700ec743deb957116f35a78148d95ac4f16 -->

```json
{
  "operationId": "list_processing_claim_disposition_outputs",
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
            "$ref": "#/components/schemas/ArtifactDispositionOutputPageDocument"
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
        "conflict"
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
  "summary": "List Processing Claim Disposition Outputs",
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
    "authority": "processing-claim-disposition-outputs",
    "authority_parameter": "authority_sha256",
    "cursor_parameter": "start_ordinal",
    "fixed_limit": 128,
    "kind": "exact-authority-page"
  }
}
```
