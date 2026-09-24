# POST /v1/run

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:a-riverhog-ftp-spool:post-v1-run:2a05e697fd -->

Run Pass

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-176b5a4c24"></a>
- <a id="s-2d7d443380"></a>`operationId`: `"run_ftp_spool_pass"`
- <a id="s-8dea3f1c02"></a>`security`: `[{"RiverhogFtpSpoolBearer":[]}]`
- <a id="s-579a1b508c"></a>`summary`: `"Run Pass"`
- <a id="s-dc3dd285eb"></a>`tags`: `["operations"]`

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-93c65270ba"></a>`200` | Successful Response | application/json | type="object"; additionalProperties=(any JSON value); title="Response Run Ftp Spool Pass" | not declared |
| <a id="s-689a381a81"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-6252582393"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-d0502b66bb"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-cb52652283"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"a-riverhog-ftp-spool"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-e9fc7bbc4d"></a>[response 200 · content · application/json](#s-93c65270ba) | `cardinality · entries · operational_policy` | shared above |

## Maintained corroboration

### Related interface records

- [a-riverhog-ftp-spool run](../cli/a-riverhog-ftp-spool-run.md)
- [a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.run_ftp_spool_pass](../../a-riverhog-ftp-spool-client/python/a-riverhog-ftp-spool-client-riverhogftpspoolclient-run-ftp-spool-pass.md)

### Referenced contract elements

- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-e04a47d973"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-591ccf3e13"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-fdb5f95db7) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/app.py::create\_app.&lt;locals&gt;.run\_pass](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/app.py#L317)

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
      "command": "run",
      "executable": "a-riverhog-ftp-spool",
      "result_identity": "a-riverhog-ftp-spool-cli-result/run/v1",
      "source": {
        "line": 440,
        "module": "a_riverhog_ftp_spool.app",
        "path": "some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/app.py",
        "symbol": "_run_command"
      }
    }
  ],
  "cli_commands": [
    "run"
  ],
  "client": "RiverhogFtpSpoolClient",
  "client_bindings": [
    {
      "public_identity": "a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.run_ftp_spool_pass",
      "source": {
        "line": 115,
        "module": "a_riverhog_ftp_spool_client.client",
        "path": "some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/client.py",
        "symbol": "RiverhogFtpSpoolClient.run_ftp_spool_pass"
      }
    }
  ],
  "method": "POST",
  "operation_id": "run_ftp_spool_pass",
  "path": "/v1/run",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/a-riverhog-ftp-spool/paths/~1v1~1run/post`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 31aa8c614aac0b6eaa82300ea6ac287ac34aa73816388696f813b815b847b838 -->

```json
{
  "operationId": "run_ftp_spool_pass",
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "additionalProperties": true,
            "title": "Response Run Ftp Spool Pass",
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
  "summary": "Run Pass",
  "tags": [
    "operations"
  ]
}
```

</details>
