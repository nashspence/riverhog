# piggity app key quota show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-app-key-quota-show:317a8ee257 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-fcc5cb4bad"></a>Parser name: `show`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-a2059db178"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-60f376f13d"></a>`help` | <a id="s-dfe79c2da7"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-c5546ae1b9"></a>`0` | <a id="s-8ec46b7d9d"></a>`"noncontractual-framework-help"` | <a id="s-e1c773ca8b"></a>`"empty"` |

### Result and failure contract

- <a id="s-b77098f96b"></a>Result identity: `piggity-cli-result/app/key/quota/show/v1`
- <a id="s-1be71b91bb"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-d594d04c45"></a>Structured output: `optional-json`
- <a id="s-2b3c71411a"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-257875ff67"></a>`completed` | <a id="s-cd0a9493a9"></a>`{"kind":"command-completed"}` | <a id="s-7197715aaa"></a>`0` | <a id="s-2996f3ed91"></a>`human: noncontractual-presentation-of-command-result; json: HTTP get_download_quota — #/components/schemas/KeyDownloadQuotaOut` | <a id="s-6b9f7347b3"></a>`all: empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-081e1c0711"></a>`usage` | <a id="s-6d843c5b23"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-308facc049"></a>`2` | <a id="s-f695aa8b7e"></a>`all: empty` | <a id="s-e5bc177144"></a>`all: noncontractual-usage-diagnostic` |
| <a id="s-0ca72af393"></a>`operational` | <a id="s-66b576b507"></a>`{"kind":"application-error"}` | <a id="s-db55a96afe"></a>`1` | <a id="s-e07019c506"></a>`human: empty; json: http-api-contracts.ErrorResponse` | <a id="s-cdb6c5f081"></a>`human: noncontractual-diagnostic; json: empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-a2059db178) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/download-quota](../../riverhog/http-operations/get-v1-download-quota.md)

## Governing policies

- <a id="pa-0aad852004"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-fb45b77394"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/app/commands/key/commands/quota/commands/show/name`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/quota/commands/show/parameters`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/quota/commands/show/result_contract`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/quota/commands/show/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/app/commands/key/commands/quota/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/quota/commands/show/parameters`

<!-- exact-contract-value: f2cf9ed04ac608b58219dbcf22fc63be2fdf35901bc058f443df21b229aefd32 -->

```json
[
  {
    "count": false,
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "json_mode",
    "nargs": 1,
    "options": [
      "--json"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
    }
  }
]
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/quota/commands/show/result_contract`

<!-- exact-contract-value: 05633a477ba70ad2a06614c335e66a8ca6ab07dda9fe80283da55f32a55adbc2 -->

```json
{
  "failures": [
    {
      "exit_status": 2,
      "id": "usage",
      "selected_by": {
        "kind": "parser-rejected-invocation"
      },
      "stderr": {
        "all": "noncontractual-usage-diagnostic"
      },
      "stdout": {
        "all": "empty"
      }
    },
    {
      "exit_status": 1,
      "id": "operational",
      "selected_by": {
        "kind": "application-error"
      },
      "stderr": {
        "human": "noncontractual-diagnostic",
        "json": "empty"
      },
      "stdout": {
        "human": "empty",
        "json": {
          "identity": "http-api-contracts.ErrorResponse",
          "kind": "python-model",
          "schema": {
            "$defs": {
              "ErrorBody": {
                "additionalProperties": false,
                "properties": {
                  "code": {
                    "minLength": 1,
                    "title": "Code",
                    "type": "string"
                  },
                  "details": {
                    "anyOf": [
                      {
                        "additionalProperties": true,
                        "type": "object"
                      },
                      {
                        "type": "null"
                      }
                    ],
                    "default": null,
                    "title": "Details"
                  },
                  "message": {
                    "minLength": 1,
                    "title": "Message",
                    "type": "string"
                  }
                },
                "required": [
                  "code",
                  "message"
                ],
                "title": "ErrorBody",
                "type": "object"
              }
            },
            "additionalProperties": false,
            "properties": {
              "error": {
                "$ref": "#/$defs/ErrorBody"
              }
            },
            "required": [
              "error"
            ],
            "title": "ErrorResponse",
            "type": "object"
          }
        }
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "piggity-cli-result/app/key/quota/show/v1",
  "profile_id": "piggity-cli-human-json/v1",
  "structured_output": "optional-json",
  "success": [
    {
      "exit_status": 0,
      "id": "completed",
      "selected_by": {
        "kind": "command-completed"
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": {
          "application": "riverhog",
          "kind": "http-operation-response",
          "method": "GET",
          "operation_id": "get_download_quota",
          "path": "/v1/download-quota",
          "schema": {
            "$ref": "#/components/schemas/KeyDownloadQuotaOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/quota/commands/show/terminating_controls`

<!-- exact-contract-value: 654ffd6937a42b17b4204e0750fd74bd42751d2241f4a4632015efb38811a79c -->

```json
[
  {
    "exit_status": 0,
    "id": "help",
    "stderr": "empty",
    "stdout": "noncontractual-framework-help",
    "trigger": {
      "kind": "option-present",
      "options": [
        "--help"
      ]
    }
  }
]
```
