# gogurt provider mounted-volume show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-provider-mounted-volume-show:836f625ce3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-85eac40052"></a>Parser name: `show`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-ae67412ef1"></a>`name` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | name |
| <a id="s-acb58db353"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-15802b4e32"></a>`help` | <a id="s-83559fb3b5"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-c62e10e314"></a>`0` | <a id="s-f85040ea48"></a>`"noncontractual-framework-help"` | <a id="s-38e718f845"></a>`"empty"` |

### Result and failure contract

- <a id="s-35b6418c64"></a>Result identity: `gogurt-cli-result/provider/mounted-volume/show/v1`
- <a id="s-156414f2b5"></a>Profile: `gogurt-cli-human-json/v1`
- <a id="s-00ed1397c1"></a>Structured output: `optional-json`
- <a id="s-903599bc43"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-82531474d0"></a>`completed` | <a id="s-9c7fffa628"></a>`{"kind":"command-completed"}` | <a id="s-eebd7952ce"></a>`0` | <a id="s-e781cfdf07"></a>human: `noncontractual-presentation-of-command-result`; json: [gogurt-provider-detail/v1](#s-e781cfdf07) | <a id="s-1d22d8a484"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-4597c7afef"></a>`usage` | <a id="s-74bc45ae39"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-4c76ac66dd"></a>`2` | <a id="s-f5c11ddb2a"></a>all: `empty` | <a id="s-cdaaf801bc"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-2a6fa2398a"></a>`operational` | <a id="s-9dd8209a13"></a>`{"kind":"application-error"}` | <a id="s-6720ff73ce"></a>`1` | <a id="s-ecad4578e9"></a>human: `empty`; json: [gogurt-cli-error/v1](#s-ecad4578e9) | <a id="s-0c70c02b19"></a>human: `noncontractual-diagnostic`; json: `empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-acb58db353) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter name](#s-ae67412ef1) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-7e32f4919d"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-d521d9b3e1"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources.md#src-3b2297c37d) — `reference/gogurt/application/src/gogurt/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/show/name`
- `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/show/parameters`
- `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/show/result_contract`
- `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/show/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/show/parameters`

<!-- exact-contract-value: 5fc7dfae38d65070ca7b7f9d59934174307d6a3212e66d1af82fe71c64370451 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "name",
    "nargs": 1,
    "options": [
      "name"
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

### `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/show/result_contract`

<!-- exact-contract-value: 4417431fcf8ca51492efc73264cd9a2d3fd4c11c26fed66635c77edcd2d6c253 -->

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
          "identity": "gogurt-cli-error/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "error": {
                "additionalProperties": false,
                "properties": {
                  "code": {
                    "enum": [
                      "config_error",
                      "listener_error"
                    ]
                  },
                  "message": {
                    "type": "string"
                  }
                },
                "required": [
                  "code",
                  "message"
                ],
                "type": "object"
              }
            },
            "required": [
              "error"
            ],
            "type": "object"
          }
        }
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "gogurt-cli-result/provider/mounted-volume/show/v1",
  "profile_id": "gogurt-cli-human-json/v1",
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
          "identity": "gogurt-provider-detail/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "distribution": {
                "type": [
                  "string",
                  "null"
                ]
              },
              "entry_point": {
                "type": "string"
              },
              "kind": {
                "enum": [
                  "mounted-volume",
                  "listener-host"
                ]
              },
              "name": {
                "type": "string"
              },
              "reference": {
                "additionalProperties": false,
                "properties": {
                  "kind": {
                    "enum": [
                      "mounted-volume",
                      "listener-host"
                    ]
                  },
                  "name": {
                    "type": "string"
                  },
                  "provider_id": {
                    "type": "string"
                  }
                },
                "required": [
                  "kind",
                  "name",
                  "provider_id"
                ],
                "type": "object"
              },
              "version": {
                "type": [
                  "string",
                  "null"
                ]
              }
            },
            "required": [
              "kind",
              "name",
              "entry_point",
              "distribution",
              "version",
              "reference"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/show/terminating_controls`

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
