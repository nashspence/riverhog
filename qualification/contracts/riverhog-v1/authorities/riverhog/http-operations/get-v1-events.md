# GET /v1/events

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:riverhog:get-v1-events:13bb12a502 -->

List Lifecycle Events

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-c816d52258"></a>
- <a id="s-417de4f5c8"></a>`operationId`: `"list_lifecycle_events"`
- <a id="s-7098fb8d49"></a>`security`: `[{"HTTPBearer":[]}]`
- <a id="s-39f46c6a53"></a>`summary`: `"List Lifecycle Events"`
- <a id="s-7a1ef6ac0f"></a>`tags`: `["events"]`
- <a id="s-3f3ff8848f"></a>`x-riverhog-permission-requirements`: `[{"any_of":["events:read"]}]`
- <a id="s-2aa886527d"></a>`x-riverhog-read-collection`: `{"cursor_parameter":"after","kind":"cursor-feed","limit_parameter":"limit"}`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-2def9a57e2"></a>`after` | query | no | not declared | anyOf=[([LifecycleEventCursor](../http-schemas/schemas-lifecycleeventcursor.md)); (type="null")]; title="After" |
| <a id="s-cb497e59ec"></a>`limit` | query | no | `100` | type="integer"; minimum=1; maximum=100; title="Limit" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-61a7a9f60f"></a>`200` | Successful Response | application/json | [RiverhogEventPage](../http-schemas/schemas-riverhogeventpage.md) | not declared |
| <a id="s-5887072ecb"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-47ddb22113"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-aad3824185"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-eb550bb43e"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: progression={"cursor_parameter":"after","kind":"cursor-feed","limit_parameter":"limit"}; reason="bounded-route-progression"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [GET /v1/events](#s-c816d52258) | `logical-result-cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-6090734935"></a>[parameter limit](#s-cb497e59ec) | `value · schema-value · contract_max` | shared above |

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

- [a-riverhog-cli event list](../../a-riverhog-cli/cli/a-riverhog-cli-event-list.md)
- [riverhog_client.ApiClient.list_lifecycle_events](../../riverhog-client/python/riverhog-client-apiclient-list-lifecycle-events.md)

### Referenced contract elements

- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)
- [schemas: LifecycleEventCursor](../http-schemas/schemas-lifecycleeventcursor.md)
- [schemas: RiverhogEventPage](../http-schemas/schemas-riverhogeventpage.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-bc38801a90"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-d8c76db3f8"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)
- <a id="pa-f92e425cf6"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [riverhog/src/riverhog\_api/routers/events.py::list\_lifecycle\_events](../../../../../../riverhog/src/riverhog_api/routers/events.py#L17)

### Structural operation bindings

This generated record links maintained client, CLI, response-authority, and provider routes. It checks interface structure, not executed qualification, successful CLI execution, or human/JSON equivalence. Test bindings and qualification commands are audit leads, not run results.

<details>
<summary>Exact structural binding record</summary>

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_bindings": [
    {
      "command": "event list",
      "executable": "a-riverhog-cli",
      "result_identity": "a-riverhog-cli-result/event/list/v1",
      "source": {
        "line": 753,
        "module": "a_riverhog_cli.main",
        "path": "some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py",
        "symbol": "event_list_cmd"
      }
    }
  ],
  "cli_commands": [
    "event list"
  ],
  "client": "ApiClient",
  "client_bindings": [
    {
      "public_identity": "riverhog_client.ApiClient.list_lifecycle_events",
      "source": {
        "line": 725,
        "module": "riverhog_client.client",
        "path": "packages/riverhog-client/src/riverhog_client/client.py",
        "symbol": "ApiClient.list_lifecycle_events"
      }
    }
  ],
  "method": "GET",
  "operation_id": "list_lifecycle_events",
  "path": "/v1/events",
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

- `/external_contract/http_openapi/riverhog/paths/~1v1~1events/get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7de43c4542433949b7dfd4ad2fce99c2f55e5dc2bb045afaab46fe0d072979b9 -->

```json
{
  "operationId": "list_lifecycle_events",
  "parameters": [
    {
      "in": "query",
      "name": "after",
      "required": false,
      "schema": {
        "anyOf": [
          {
            "$ref": "#/components/schemas/LifecycleEventCursor"
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
            "$ref": "#/components/schemas/RiverhogEventPage"
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
  "summary": "List Lifecycle Events",
  "tags": [
    "events"
  ],
  "x-riverhog-permission-requirements": [
    {
      "any_of": [
        "events:read"
      ]
    }
  ],
  "x-riverhog-read-collection": {
    "cursor_parameter": "after",
    "kind": "cursor-feed",
    "limit_parameter": "limit"
  }
}
```

</details>
