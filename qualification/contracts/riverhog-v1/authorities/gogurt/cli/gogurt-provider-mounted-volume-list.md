# gogurt provider mounted-volume list

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-provider-mounted-volume-list:97dcc49c26 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-b69581520f"></a>Parser name: `list`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-39f1dc9eb2"></a>`ids` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --ids |
| <a id="s-20a1505b65"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-915aa1f759"></a>`help` | <a id="s-18c1d33322"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-2cc5a4825e"></a>`0` | <a id="s-16ea523617"></a>`"noncontractual-framework-help"` | <a id="s-8c3df9bb4f"></a>`"empty"` |

### Result and failure contract

- <a id="s-cd531fdc6d"></a>Result identity: `gogurt-cli-result/provider/mounted-volume/list/v1`
- <a id="s-46a8079d5a"></a>Profile: `gogurt-cli-human-json/v1`
- <a id="s-1583142758"></a>Structured output: `optional-json`
- <a id="s-3f4c055c08"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-cf8f2c4504"></a>`completed` | <a id="s-50f315949d"></a>`{"kind":"command-completed"}` | <a id="s-cc1b9f9be2"></a>`0` | <a id="s-6425bca83c"></a>`human: noncontractual-presentation-of-command-result; json: gogurt-provider-list/v1` | <a id="s-536936b94e"></a>`all: empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-32cb5a3c52"></a>`usage` | <a id="s-b6eae10f8d"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-8115df8409"></a>`2` | <a id="s-a944f4f084"></a>`all: empty` | <a id="s-bebd7a001c"></a>`all: noncontractual-usage-diagnostic` |
| <a id="s-ccfca921f6"></a>`operational` | <a id="s-72e98de6a5"></a>`{"kind":"application-error"}` | <a id="s-ec8d487fe3"></a>`1` | <a id="s-1c58824587"></a>`human: empty; json: gogurt-cli-error/v1` | <a id="s-71a4576897"></a>`human: noncontractual-diagnostic; json: empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --ids](#s-39f1dc9eb2) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-20a1505b65) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-39d84a35de"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-d60cf02d66"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources.md#src-3b2297c37d) — `reference/gogurt/application/src/gogurt/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/list/name`
- `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/list/parameters`
- `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/list/result_contract`
- `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/list/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/list/name`

<!-- exact-contract-value: dcb452a982945e5e2957930d83d36af5ceee19805ec0c3b30529ae8f44f6e49e -->

```json
"list"
```

### `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/list/parameters`

<!-- exact-contract-value: e1d69a72acbf63fdc3459d233dcdf028761244a8bf1785d2a24bd76449d6da79 -->

```json
[
  {
    "count": false,
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "ids",
    "nargs": 1,
    "options": [
      "--ids"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
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

### `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/list/result_contract`

<!-- exact-contract-value: 7272927dd7761ba4ac6fab540d168dbbdd36483168d6a14876bb672e92f01d4c -->

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
  "identity": "gogurt-cli-result/provider/mounted-volume/list/v1",
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
          "identity": "gogurt-provider-list/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "format": {
                "const": "gogurt-provider-list/v1"
              },
              "kind": {
                "enum": [
                  "mounted-volume",
                  "listener-host"
                ]
              },
              "providers": {
                "items": {
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
                    "version"
                  ],
                  "type": "object"
                },
                "type": "array"
              }
            },
            "required": [
              "format",
              "kind",
              "providers"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/gogurt/commands/provider/commands/mounted-volume/commands/list/terminating_controls`

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
