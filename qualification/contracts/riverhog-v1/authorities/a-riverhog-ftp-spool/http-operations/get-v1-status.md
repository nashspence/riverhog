# GET /v1/status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:a-riverhog-ftp-spool:get-v1-status:874d813d73 -->

Status

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-4951dae8ce"></a>
- <a id="s-447ab5568b"></a>`operationId`: `"get_ftp_spool_status"`
- <a id="s-4119ac1cc9"></a>`security`: `[{"RiverhogFtpSpoolBearer":[]}]`
- <a id="s-65da41d1a5"></a>`summary`: `"Status"`
- <a id="s-576b64bd95"></a>`tags`: `["service"]`
- <a id="s-ae4754df5c"></a>`x-riverhog-read-collection`: `{"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-22b8e9be0c"></a>`page_size` | query | no | `25` | type="integer"; minimum=1; maximum=100; title="Page Size" |
| <a id="s-d56b465250"></a>`page_token` | query | no | not declared | anyOf=[(type="string"; maxLength=120; minLength=1); (type="null")]; title="Page Token" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-9461dfcf79"></a>`200` | Successful Response | application/json | type="object"; additionalProperties=(any JSON value); title="Response Get Ftp Spool Status" | not declared |
| <a id="s-550c9b0b45"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-2f174f4cd6"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-93845e0d8a"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-ff5cecd357"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"a-riverhog-ftp-spool"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-b52068b2d3"></a>[response 200 · content · application/json](#s-9461dfcf79) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/status](#s-4951dae8ce) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-9882dd735b"></a>[parameter page_size](#s-22b8e9be0c) | `value · schema-value · contract_max` | maximum=100 |
| <a id="s-1dcfce02c0"></a>[parameter page_token · string value](#s-d56b465250) | `length · characters · contract_max` | maximum=120 |

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

- [a-riverhog-ftp-spool-status-progression/v1](../../../evidence/qualifications/a-riverhog-ftp-spool-status-progression-v1/index.md)

## Maintained corroboration

### Related interface records

- [a-riverhog-ftp-spool status](../cli/a-riverhog-ftp-spool-status.md)
- [a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.get_ftp_spool_status](../../a-riverhog-ftp-spool-client/python/a-riverhog-ftp-spool-client-riverhogftpspoolclient-get-ftp-spool-status.md)

### Referenced contract elements

- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-54016ca0e2"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-4aaaf8c17a"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-10323a440b"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)
- <a id="pa-4c16dde080"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-fdb5f95db7) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/app.py::create\_app.&lt;locals&gt;.status](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/app.py#L270)

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

<details>
<summary>Exact structural binding record</summary>

```json
{
  "application": "a-riverhog-ftp-spool",
  "classification": "human-cli+json",
  "cli_bindings": [
    {
      "command": "status",
      "executable": "a-riverhog-ftp-spool",
      "result_identity": "a-riverhog-ftp-spool-cli-result/status/v1",
      "source": {
        "line": 395,
        "module": "a_riverhog_ftp_spool.app",
        "path": "some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/app.py",
        "symbol": "_status_command"
      }
    }
  ],
  "cli_commands": [
    "status"
  ],
  "client": "RiverhogFtpSpoolClient",
  "client_bindings": [
    {
      "public_identity": "a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.get_ftp_spool_status",
      "source": {
        "line": 86,
        "module": "a_riverhog_ftp_spool_client.client",
        "path": "some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/client.py",
        "symbol": "RiverhogFtpSpoolClient.get_ftp_spool_status"
      }
    }
  ],
  "method": "GET",
  "operation_id": "get_ftp_spool_status",
  "path": "/v1/status",
  "provider_evidence": null,
  "read_collection": {
    "default_page_size": 25,
    "kind": "mutable-browse",
    "maximum_page_size": 100,
    "next_page_token_field": "next_page_token",
    "page_size_parameter": "page_size",
    "page_token_parameter": "page_token"
  },
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/a-riverhog-ftp-spool/paths/~1v1~1status/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 93a29d33335333bea8d278280c2874ec7c6638d77f144207355bdf39667323cd -->

```json
{
  "operationId": "get_ftp_spool_status",
  "parameters": [
    {
      "in": "query",
      "name": "page_size",
      "required": false,
      "schema": {
        "default": 25,
        "maximum": 100,
        "minimum": 1,
        "title": "Page Size",
        "type": "integer"
      }
    },
    {
      "in": "query",
      "name": "page_token",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "maxLength": 120,
            "minLength": 1,
            "type": "string"
          },
          {
            "type": "null"
          }
        ],
        "title": "Page Token"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "additionalProperties": true,
            "title": "Response Get Ftp Spool Status",
            "type": "object"
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
      "RiverhogFtpSpoolBearer": []
    }
  ],
  "summary": "Status",
  "tags": [
    "service"
  ],
  "x-riverhog-read-collection": {
    "default_page_size": 25,
    "kind": "mutable-browse",
    "maximum_page_size": 100,
    "next_page_token_field": "next_page_token",
    "page_size_parameter": "page_size",
    "page_token_parameter": "page_token"
  }
}
```

</details>
