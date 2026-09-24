# GET /v1/sources/{source_id}/events

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:a-riverhog-ftp-spool:get-v1-sources-source-id-events:f70bba5259 -->

Events

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-7c18f6a5b8"></a>
- <a id="s-da52dbde61"></a>`description`: `"Each source retains events for the lifetime of its operational state. Cursors are bound to that source and state generation; a reset or cross-source cursor is rejected rather than silently skipping history."`
- <a id="s-c11d00a0e0"></a>`operationId`: `"list_ftp_spool_events"`
- <a id="s-5cf9fc67ce"></a>`security`: `[{"RiverhogFtpSpoolBearer":[]}]`
- <a id="s-8b33fa9b13"></a>`summary`: `"Events"`
- <a id="s-ccf96a23a4"></a>`tags`: `["events"]`
- <a id="s-551e80cfca"></a>`x-riverhog-read-collection`: `{"cursor_parameter":"after","kind":"cursor-feed","limit_parameter":"limit"}`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-f71ce4c996"></a>`source_id` | path | yes | not declared | type="string"; title="Source Id" |
| <a id="s-250d80ce78"></a>`after` | query | no | not declared | anyOf=[(type="string"; maxLength=220; minLength=1); (type="null")]; title="After" |
| <a id="s-c0fed25a54"></a>`limit` | query | no | `100` | type="integer"; minimum=1; maximum=100; title="Limit" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-b7e636c45c"></a>`200` | Successful Response | application/json | [FtpEventPage](../http-schemas/schemas-ftpeventpage.md) | not declared |
| <a id="s-5ab199adbc"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-353c354a0a"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-8b9d558f4f"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-0c48705e55"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: progression={"cursor_parameter":"after","kind":"cursor-feed","limit_parameter":"limit"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/sources/{source_id}/events](#s-7c18f6a5b8) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-9c13d27f46"></a>[parameter after · string value](#s-250d80ce78) | `length · characters · contract_max` | maximum=220 |
| <a id="s-da39ca58bb"></a>[parameter limit](#s-c0fed25a54) | `value · schema-value · contract_max` | maximum=100 |

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

- [a-riverhog-ftp-spool-read-collection-progression/v1](../../../evidence/qualifications/a-riverhog-ftp-spool-read-collection-progression-v1/index.md)

## Maintained corroboration

### Related interface records

- [a-riverhog-ftp-spool events](../cli/a-riverhog-ftp-spool-events.md)
- [a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.list_ftp_spool_events](../../a-riverhog-ftp-spool-client/python/a-riverhog-ftp-spool-client-riverhogftpspoolclient-list-ftp-spool-events.md)

### Referenced contract elements

- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)
- [schemas: FtpEventPage](../http-schemas/schemas-ftpeventpage.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-ca0c824245"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-64630ca2df"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)
- <a id="pa-98a19bfc5b"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-fdb5f95db7) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/app.py::create\_app.&lt;locals&gt;.events](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/app.py#L290)

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
      "command": "events",
      "executable": "a-riverhog-ftp-spool",
      "result_identity": "a-riverhog-ftp-spool-cli-result/events/v1",
      "source": {
        "line": 455,
        "module": "a_riverhog_ftp_spool.app",
        "path": "some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/app.py",
        "symbol": "_events_command"
      }
    }
  ],
  "cli_commands": [
    "events"
  ],
  "client": "RiverhogFtpSpoolClient",
  "client_bindings": [
    {
      "public_identity": "a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.list_ftp_spool_events",
      "source": {
        "line": 100,
        "module": "a_riverhog_ftp_spool_client.client",
        "path": "some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/client.py",
        "symbol": "RiverhogFtpSpoolClient.list_ftp_spool_events"
      }
    }
  ],
  "method": "GET",
  "operation_id": "list_ftp_spool_events",
  "path": "/v1/sources/{source_id}/events",
  "provider_evidence": null,
  "read_collection": {
    "cursor_parameter": "after",
    "kind": "cursor-feed",
    "limit_parameter": "limit"
  },
  "response_authority": "canonical-document"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/a-riverhog-ftp-spool/paths/~1v1~1sources~1{source_id}~1events/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8c062ffda44b7dcc2efce5bc05ac4e3558386922063c0db39a06b4e4de0de688 -->

```json
{
  "description": "Each source retains events for the lifetime of its operational state. Cursors are bound to that source and state generation; a reset or cross-source cursor is rejected rather than silently skipping history.",
  "operationId": "list_ftp_spool_events",
  "parameters": [
    {
      "in": "path",
      "name": "source_id",
      "required": true,
      "schema": {
        "title": "Source Id",
        "type": "string"
      }
    },
    {
      "in": "query",
      "name": "after",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "maxLength": 220,
            "minLength": 1,
            "type": "string"
          },
          {
            "type": "null"
          }
        ],
        "title": "After"
      }
    },
    {
      "in": "query",
      "name": "limit",
      "required": false,
      "schema": {
        "default": 100,
        "maximum": 100,
        "minimum": 1,
        "title": "Limit",
        "type": "integer"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/FtpEventPage"
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
  "summary": "Events",
  "tags": [
    "events"
  ],
  "x-riverhog-read-collection": {
    "cursor_parameter": "after",
    "kind": "cursor-feed",
    "limit_parameter": "limit"
  }
}
```

</details>
