# POST /v1/admission-policies/{policy_id}:backfill

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:stove0:post-v1-admission-policies-policy-id-backfill:b2144d91fb -->

Backfill Admission Policy

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-a03a786413"></a>
- <a id="s-4b5fd3fd50"></a>`operationId`: `"backfill_admission_policy"`
- <a id="s-51b7a7f933"></a>`summary`: `"Backfill Admission Policy"`
- <a id="s-80def6a78d"></a>`tags`: `["admissions"]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-b6cc543640"></a>`policy_id` | path | yes | not declared | type="string"; title="Policy Id" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-c589a46b03"></a>`200` | Successful Response | application/json | [AdmissionPolicyStatus](../http-schemas/schemas-admissionpolicystatus.md) | not declared |
| <a id="s-33d86e5074"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-96370e1b48"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-15c08c80dd"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-06c122ae8e"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

## Maintained corroboration

### Related interface records

- [stove0 admission policy backfill](../../a-stove0-cli/cli/stove0-admission-policy-backfill.md)
- [stove0_api_client.Stove0ApiClient.backfill_admission_policy](../../stove0-api-client/python/stove0-api-client-stove0apiclient-backfill-admission-policy.md)

### Referenced contract elements

- [schemas: AdmissionPolicyStatus](../http-schemas/schemas-admissionpolicystatus.md)
- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)

## Governing policies

- <a id="pa-a52842816b"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [some-implementations/stove0/application/server/src/stove0\_api/app.py::create\_app.&lt;locals&gt;.backfill\_admission\_policy](../../../../../../some-implementations/stove0/application/server/src/stove0_api/app.py#L593)

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
      "command": "admission policy backfill",
      "executable": "stove0",
      "result_identity": "stove0-cli-result/admission/policy/backfill/v1",
      "source": {
        "line": 201,
        "module": "a_stove0_cli.main",
        "path": "some-implementations/stove0/application/client/src/a_stove0_cli/main.py",
        "symbol": "backfill_admission_policy"
      }
    }
  ],
  "cli_commands": [
    "admission policy backfill"
  ],
  "client": "Stove0ApiClient",
  "client_bindings": [
    {
      "public_identity": "stove0_api_client.Stove0ApiClient.backfill_admission_policy",
      "source": {
        "line": 173,
        "module": "stove0_api_client.client",
        "path": "some-implementations/stove0/packages/api-client/src/stove0_api_client/client.py",
        "symbol": "Stove0ApiClient.backfill_admission_policy"
      }
    }
  ],
  "method": "POST",
  "operation_id": "backfill_admission_policy",
  "path": "/v1/admission-policies/{policy_id}:backfill",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1admission-policies~1{policy_id}:backfill/post`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4854ae69c6d39543b6f573cc06a354ec3d9ec222454f77c53ccde1f87f105f05 -->

```json
{
  "operationId": "backfill_admission_policy",
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
            "$ref": "#/components/schemas/AdmissionPolicyStatus"
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
  "summary": "Backfill Admission Policy",
  "tags": [
    "admissions"
  ]
}
```

</details>
