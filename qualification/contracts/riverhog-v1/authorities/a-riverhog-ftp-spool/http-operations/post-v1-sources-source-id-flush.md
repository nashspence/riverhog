# POST /v1/sources/{source_id}/flush

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-operations:a-riverhog-ftp-spool:post-v1-sources-source-id-flush:91b6ce215b -->

Flush

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [HTTP Operations](index.md) |

## External contract

<a id="s-a0a122dc67"></a>
- <a id="s-58a26e9107"></a>`operationId`: `"flush_ftp_spool_source"`
- <a id="s-acef9e4648"></a>`security`: `[{"RiverhogFtpSpoolBearer":[]}]`
- <a id="s-874da0ad63"></a>`summary`: `"Flush"`
- <a id="s-2d2924511a"></a>`tags`: `["operations"]`

### Parameters

| Name | In | Required | Default | Schema |
|---|---|---:|---|---|
| <a id="s-8986fdc4f3"></a>`source_id` | path | yes | not declared | type="string"; title="Source Id" |

### Responses

| Status | Description | Media type | Schema | Declared error codes |
|---|---|---|---|---|
| <a id="s-9794a3c643"></a>`200` | Successful Response | application/json | type="object"; additionalProperties=(any JSON value); title="Response Flush Ftp Spool Source" | not declared |
| <a id="s-13078b20c8"></a>`400` | Bad Request | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `bad_request` |
| <a id="s-41bdcf4a26"></a>`401` | Unauthorized | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `unauthorized` |
| <a id="s-1c625d0ef0"></a>`403` | Forbidden | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `forbidden` |
| <a id="s-81d4bb5d1c"></a>`500` | Internal Server Error | application/json | [ErrorOut](../http-schemas/schemas-errorout.md) | `internal_error` |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"a-riverhog-ftp-spool"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-876e0b5d27"></a>[response 200 · content · application/json](#s-9794a3c643) | `cardinality · entries · operational_policy` | shared above |

## Maintained corroboration

### Related interface records

- [a-riverhog-ftp-spool flush](../cli/a-riverhog-ftp-spool-flush.md)
- [a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.flush_ftp_spool_source](../../a-riverhog-ftp-spool-client/python/a-riverhog-ftp-spool-client-riverhogftpspoolclient-flush-ftp-spool-source.md)

### Referenced contract elements

- [schemas: ErrorOut](../http-schemas/schemas-errorout.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-dbf25d69f2"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-4ee4ea0c4c"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-fdb5f95db7) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)
- [operations:operation-matrix](../../../evidence/sources/authorities.md#src-b032bdc56b) — [scripts/operation\_qualification.py::operation\_matrix](../../../../../../scripts/operation_qualification.py)
- **Handler:** [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/app.py::create\_app.&lt;locals&gt;.flush](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/app.py#L326)

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
      "command": "flush",
      "executable": "a-riverhog-ftp-spool",
      "result_identity": "a-riverhog-ftp-spool-cli-result/flush/v1",
      "source": {
        "line": 461,
        "module": "a_riverhog_ftp_spool.app",
        "path": "some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/app.py",
        "symbol": "_flush_command"
      }
    }
  ],
  "cli_commands": [
    "flush"
  ],
  "client": "RiverhogFtpSpoolClient",
  "client_bindings": [
    {
      "public_identity": "a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.flush_ftp_spool_source",
      "source": {
        "line": 118,
        "module": "a_riverhog_ftp_spool_client.client",
        "path": "some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/client.py",
        "symbol": "RiverhogFtpSpoolClient.flush_ftp_spool_source"
      }
    }
  ],
  "method": "POST",
  "operation_id": "flush_ftp_spool_source",
  "path": "/v1/sources/{source_id}/flush",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```

</details>

### Machine authority

- `/external_contract/http_openapi/a-riverhog-ftp-spool/paths/~1v1~1sources~1{source_id}~1flush/post`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2d05d8f7a8298feb6977d1b7fdec1baea49bbab32c8c6b699609ebbae90edc57 -->

```json
{
  "operationId": "flush_ftp_spool_source",
  "parameters": [
    {
      "in": "path",
      "name": "source_id",
      "required": true,
      "schema": {
        "title": "Source Id",
        "type": "string"
      }
    }
  ],
  "responses": {
    "200": {
      "content": {
        "application/json": {
          "schema": {
            "additionalProperties": true,
            "title": "Response Flush Ftp Spool Source",
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
  "summary": "Flush",
  "tags": [
    "operations"
  ]
}
```

</details>
