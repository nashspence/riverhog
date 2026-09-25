# POST /v1/departure-policies/{policy_id}:rebaseline

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:stove0:post-v1-departure-policies-policy-id-rebaseline:40b4897508 -->

Rebaseline Departure Policy

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-d1d1388766"></a>
- <a id="s-8079e2e2f2"></a>`operationId`: `"rebaseline_departure_policy"`
- <a id="s-ade27e1a88"></a>`summary`: `"Rebaseline Departure Policy"`
- <a id="s-bb21514797"></a>`tags`: `["departures"]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-024f425a8b"></a>`policy_id` | path | yes | not declared | type="string"; title="Policy Id" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-6600a6f1e7"></a>`200` | Successful Response | application/json | [DeparturePolicyStatus](../http-schemas/schemas-departurepolicystatus.md) | not declared |
| <a id="s-18d180890b"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-827a34d874"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-cf7ff1ac9a"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-175e7b0787"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [stove0 departure policy rebaseline](../../a-stove0-cli/cli/stove0-departure-policy-rebaseline.md)
- [stove0_api_client.Stove0ApiClient.rebaseline_departure_policy](../../stove0-api-client/python/stove0-api-client-stove0apiclient-rebaseline-departure-policy.md)

### Referenced contract elements

- [schemas: DeparturePolicyStatus](../http-schemas/schemas-departurepolicystatus.md)
- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)

## Governing policies

- <a id="pa-9c5485f80b"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [some-implementations/stove0/application/server/src/stove0\_api/app.py::create\_app.&lt;locals&gt;.rebaseline\_departure\_policy](../../../../../../some-implementations/stove0/application/server/src/stove0_api/app.py#L703)

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
      "command": "departure policy rebaseline",
      "executable": "stove0",
      "result_identity": "stove0-cli-result/departure/policy/rebaseline/v1",
      "source": {
        "line": 266,
        "module": "a_stove0_cli.main",
        "path": "some-implementations/stove0/application/client/src/a_stove0_cli/main.py",
        "symbol": "rebaseline_departure_policy"
      }
    }
  ],
  "cli_commands": [
    "departure policy rebaseline"
  ],
  "client": "Stove0ApiClient",
  "client_bindings": [
    {
      "public_identity": "stove0_api_client.Stove0ApiClient.rebaseline_departure_policy",
      "source": {
        "line": 228,
        "module": "stove0_api_client.client",
        "path": "some-implementations/stove0/packages/api-client/src/stove0_api_client/client.py",
        "symbol": "Stove0ApiClient.rebaseline_departure_policy"
      }
    }
  ],
  "method": "POST",
  "operation_id": "rebaseline_departure_policy",
  "path": "/v1/departure-policies/{policy_id}:rebaseline",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1departure-policies~1{policy_id}:rebaseline/post`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 22fff9cd2ad73c90d00d6708401b3cd0a224d0219d798639c3abe294ca83c4a7 -->

```json
{
  "operationId": "rebaseline_departure_policy",
  "parameters": [
    {
      "in": "path",
      "name": "policy_id",
      "required": true,
      "schema": {
        "title": "Policy Id",
        "type": "string"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "$ref": "#/components/schemas/DeparturePolicyStatus"
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
  "summary": "Rebaseline Departure Policy",
  "tags": [
    "departures"
  ]
}
```

</details>
