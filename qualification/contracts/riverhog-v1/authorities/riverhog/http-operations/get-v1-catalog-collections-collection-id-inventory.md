# GET /v1/catalog/collections/{collection_id}/inventory

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-catalog-collections-collection-id-inventory:34966b47df -->

Get Portable Collection Inventory

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-cbec0de737"></a>
- <a id="s-209130ba65"></a>`operationId`: `"get_portable_collection_inventory"`
- <a id="s-dd5b3ff2f4"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-b2ccb379bb"></a>`summary`: `"Get Portable Collection Inventory"`
- <a id="s-30a447b159"></a>`tags`: `["catalog"]`
- <a id="s-67edfcc4e1"></a>`x-riverhog-interface`: `"standard-tool/protocol"`
- <a id="s-4c09b60125"></a>`x-riverhog-permission-requirements`: `[{"any_of":["catalog:read"]}]`
- <a id="s-4d58eb9f45"></a>`x-riverhog-read-collection`: `{"authority":"portable-collection-inventory","cursor_parameter":"cursor","kind":"exact-set-page","limit_parameter":"limit","validator_header":"If-Match"}`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-d4fa758cae"></a>`collection_id` | path | yes | not declared | allOf=[(type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])"); (not=(const="0"))]; title="Collection Id" |
| <a id="s-d8cb38ecac"></a>`cursor` | query | no | not declared | anyOf=[(type="string"; maxLength=8192; minLength=1); (type="null")]; title="Cursor" |
| <a id="s-86388b9ad1"></a>`limit` | query | no | `100` | type="integer"; minimum=1; maximum=1000; title="Limit" |
| <a id="s-7620688610"></a>`If-Match` | header | no | not declared | anyOf=[(type="string"; pattern="^\"[0-9a-f]{64}\"$"); (type="null")]; title="If-Match" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-4c657abee9"></a>`200` | Successful Response | application/json | [PortableCollectionInventoryPage](../http-schemas/schemas-portablecollectioninventorypage.md) | not declared |
| <a id="s-0acee23acf"></a>`400` | Bad Request | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `bad_request` |
| <a id="s-693d94ba8f"></a>`401` | Unauthorized | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `unauthorized` |
| <a id="s-f1be810f81"></a>`403` | Forbidden | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `forbidden` |
| <a id="s-68d71c6c15"></a>`404` | Not Found | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `not_found` |
| <a id="s-b8b057a58d"></a>`412` | Precondition Failed | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `precondition_failed` |
| <a id="s-3158a7f072"></a>`428` | Precondition Required | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `precondition_required` |
| <a id="s-7940c10130"></a>`500` | Internal Server Error | application/json | [ErrorResponse](../http-schemas/schemas-errorresponse.md) | `internal_error` |

#### Response headers

| Status | Header | Required | Schema | Description |
|---|---|---|---|---|
| `200` | <a id="s-5df89ad5dc"></a>`ETag` | not declared | type="string"; pattern="^\"[0-9a-f]{64}\"$" | Strong identity of the immutable inventory authority. |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: progression={"authority":"portable-collection-inventory","cursor_parameter":"cursor","kind":"exact-set-page","limit_parameter":"limit","validator_header":"If-Match"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/catalog/collections/{collection_id}/inventory](#s-cbec0de737) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-e0db9ba9c7"></a>[parameter cursor · string value](#s-d8cb38ecac) | `length · characters · contract_max` | maximum=8192 |
| <a id="s-b623c0150e"></a>[parameter limit](#s-86388b9ad1) | `value · schema-value · contract_max` | maximum=1000 |

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

- [a-riverhog-cli local add](../../a-riverhog-cli/cli/a-riverhog-cli-local-add.md)
- [a-riverhog-cli local repair](../../a-riverhog-cli/cli/a-riverhog-cli-local-repair.md)
- [a-riverhog-cli local sync](../../a-riverhog-cli/cli/a-riverhog-cli-local-sync.md)
- [riverhog_client.ApiClient.get_portable_collection_inventory](../../riverhog-client/python/riverhog-client-apiclient-get-portable-collection-inventory.md)

### Referenced contract elements

- [schemas: ErrorResponse](../http-schemas/schemas-errorresponse.md)
- [schemas: PortableCollectionInventoryPage](../http-schemas/schemas-portablecollectioninventorypage.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-9900f268b4"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-cf99e9e632"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)
- <a id="pa-eb6f38f9ea"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/portable\_collections.py::get\_portable\_collection\_inventory](../../../../../../riverhog/src/riverhog_api/routers/portable_collections.py#L23)

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

<details>
<summary>Exact structural binding record</summary>

```json
{
  "application": "riverhog",
  "classification": "standard-tool/protocol",
  "cli_bindings": [
    {
      "command": "local add",
      "executable": "a-riverhog-cli",
      "result_identity": "a-riverhog-cli-result/local/add/v1",
      "source": {
        "line": 902,
        "module": "a_riverhog_cli.local",
        "path": "some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/local.py",
        "symbol": "add_collection"
      }
    },
    {
      "command": "local sync",
      "executable": "a-riverhog-cli",
      "result_identity": "a-riverhog-cli-result/local/sync/v1",
      "source": {
        "line": 1103,
        "module": "a_riverhog_cli.local",
        "path": "some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/local.py",
        "symbol": "sync"
      }
    },
    {
      "command": "local repair",
      "executable": "a-riverhog-cli",
      "result_identity": "a-riverhog-cli-result/local/repair/v1",
      "source": {
        "line": 1124,
        "module": "a_riverhog_cli.local",
        "path": "some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/local.py",
        "symbol": "repair"
      }
    }
  ],
  "cli_commands": [
    "local add",
    "local repair",
    "local sync"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.get_portable_collection_inventory",
      "source": {
        "line": 774,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.get_portable_collection_inventory"
      }
    }
  ],
  "method": "GET",
  "operation_id": "get_portable_collection_inventory",
  "path": "/v1/catalog/collections/{collection_id}/inventory",
  "provider_evidence": null,
  "read_collection": {
    "authority": "portable-collection-inventory",
    "cursor_parameter": "cursor",
    "kind": "exact-set-page",
    "limit_parameter": "limit",
    "validator_header": "If-Match"
  },
  "response_authority": "canonical-document"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1catalog~1collections~1{collection_id}~1inventory/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d22e8eaa9fc51dba019f83c23e83ee0fc09b9434eafb3ee41d8003ac3a6ef56c -->

```json
{
  "operationId": "get_portable_collection_inventory",
  "parameters": [
    {
      "in": "path",
      "name": "collection_id",
      "required": true,
      "schema": {
        "allOf": [
          {
            "pattern": "^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[0-1][0-9]{17}|92[0-1][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[0-1][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6][0-9]{0}|9223372036854775807)(?![\\s\\S])",
            "type": "string"
          },
          {
            "not": {
              "const": "0"
            }
          }
        ],
        "title": "Collection Id"
      }
    },
    {
      "in": "query",
      "name": "cursor",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "maxLength": 8192,
            "minLength": 1,
            "type": "string"
          },
          {
            "type": "null"
          }
        ],
        "title": "Cursor"
      }
    },
    {
      "in": "query",
      "name": "limit",
      "required": false,
      "schema": {
        "default": 100,
        "maximum": 1000,
        "minimum": 1,
        "title": "Limit",
        "type": "integer"
      }
    },
    {
      "in": "header",
      "name": "If-Match",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "pattern": "^\"[0-9a-f]{64}\"$",
            "type": "string"
          },
          {
            "type": "null"
          }
        ],
        "title": "If-Match"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/PortableCollectionInventoryPage"
          }
        }
      },
      "description": "Successful Response",
      "headers": {
        "ETag": {
          "description": "Strong identity of the immutable inventory authority.",
          "schema": {
            "pattern": "^\"[0-9a-f]{64}\"$",
            "type": "string"
          }
        }
      }
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
    "412": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Precondition Failed",
      "x-riverhog-error-codes": [
        "precondition_failed"
      ]
    },
    "428": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/ErrorResponse"
          }
        }
      },
      "description": "Precondition Required",
      "x-riverhog-error-codes": [
        "precondition_required"
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
  "summary": "Get Portable Collection Inventory",
  "tags": [
    "catalog"
  ],
  "x-riverhog-interface": "standard-tool/protocol",
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "catalog:read"
      ]
    }
  ],
  "x-riverhog-read-collection": {
    "authority": "portable-collection-inventory",
    "cursor_parameter": "cursor",
    "kind": "exact-set-page",
    "limit_parameter": "limit",
    "validator_header": "If-Match"
  }
}
```

</details>
