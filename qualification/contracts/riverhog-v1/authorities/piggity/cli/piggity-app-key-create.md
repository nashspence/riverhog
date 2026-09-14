# piggity app key create

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-app-key-create:389688df01 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-df5272f584"></a>Parser name: `create`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-915c20851a"></a>`app_name` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | app_name |
| <a id="s-0c2c77e0a6"></a>`allow` | TyperOption | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --allow |
| <a id="s-7e21fddcb2"></a>`expires_in` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --expires-in |
| <a id="s-389739a812"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-c4f829d02b"></a>`help` | <a id="s-d5d83dad72"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-45392ae491"></a>`0` | <a id="s-f859bd6c18"></a>`"noncontractual-framework-help"` | <a id="s-6a06e5aeb1"></a>`"empty"` |

### Result and failure contract

- <a id="s-ba45a5f349"></a>Result identity: `piggity-cli-result/app/key/create/v1`
- <a id="s-126dca08b4"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-19fcd3d5d8"></a>Structured output: `optional-json`
- <a id="s-e398884ee1"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-25025c7991"></a>`completed` | <a id="s-2391276cf2"></a>`{"kind":"command-completed"}` | <a id="s-aad8620f4d"></a>`0` | <a id="s-a8f63c83cf"></a>human: `noncontractual-presentation-of-command-result`; json: [HTTP create_app_key response 200](../../riverhog/http-operations/post-v1-apps-app-keys.md#s-642e16127e) | <a id="s-c54f0e928a"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-c5ea356348"></a>`usage` | <a id="s-411ea9a24c"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-c7db24a29a"></a>`2` | <a id="s-3db518e142"></a>all: `empty` | <a id="s-4c4eb50a00"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-7c1cfa0b3a"></a>`operational` | <a id="s-48d3b4ca17"></a>`{"kind":"application-error"}` | <a id="s-b4c6e7900e"></a>`1` | <a id="s-f247165e5f"></a>human: `empty`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-19b7d3244e"></a>human: `noncontractual-diagnostic`; json: `empty` |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"piggity"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --allow](#s-0c2c77e0a6) | `cardinality · occurrences · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --allow](#s-0c2c77e0a6) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter app_name](#s-915c20851a) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --expires-in](#s-7e21fddcb2) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-389739a812) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [POST /v1/apps/{app}/keys](../../riverhog/http-operations/post-v1-apps-app-keys.md)

## Governing policies

- <a id="pa-c075aa50b5"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-5587d1e724"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-8685d62e18"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/app/commands/key/commands/create/name`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/create/parameters`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/create/result_contract`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/create/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/app/commands/key/commands/create/name`

<!-- exact-contract-value: 5498a731a187f424a5800943afcba027f3a6cd684e38fe6e40c02bee1753152d -->

```json
"create"
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/create/parameters`

<!-- exact-contract-value: eeb2732f492dc62d062ab5de787037cf5ad08f71627de03805257de5a00069ac -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "app_name",
    "nargs": 1,
    "options": [
      "app_name"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": true,
    "name": "allow",
    "nargs": 1,
    "options": [
      "--allow"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "expires_in",
    "nargs": 1,
    "options": [
      "--expires-in"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  },
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

### `/external_contract/cli/piggity/commands/app/commands/key/commands/create/result_contract`

<!-- exact-contract-value: ab62314ec9b917f23409ab60b276dc18bbfe4ba862860feaa3bda60637c97d8f -->

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
  "identity": "piggity-cli-result/app/key/create/v1",
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
          "method": "POST",
          "operation_id": "create_app_key",
          "path": "/v1/apps/{app}/keys",
          "schema": {
            "$ref": "#/components/schemas/AppKeyCreatedOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/create/terminating_controls`

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
