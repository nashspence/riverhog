# gogurt write

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-write:3ebfacd4bc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-8595e8e5ab"></a>Parser name: `write`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-e3296e8a22"></a>`route` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | route |
| <a id="s-08581ea9b7"></a>`mount_point` | TyperArgument | yes | {'class': 'typer.models.TyperPath', 'name': 'path'} | mount_point |
| <a id="s-f0b9492d8e"></a>`config` | TyperOption | no | {'class': 'typer.models.TyperPath', 'name': 'path'} | --config |
| <a id="s-aba7dbf38e"></a>`force` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --force |
| <a id="s-cef35a84fe"></a>`dry_run` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --dry-run |
| <a id="s-d2fbb045b4"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |
| <a id="s-5d3f067691"></a>`mounted_volume_provider` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --mounted-volume-provider |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-937dc4eb9a"></a>`help` | <a id="s-b41616d34f"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-67992bec55"></a>`0` | <a id="s-ab4077877a"></a>`"noncontractual-framework-help"` | <a id="s-320a50b1f4"></a>`"empty"` |

### Result and failure contract

- <a id="s-e984cc1acf"></a>Result identity: `gogurt-cli-result/write/v1`
- <a id="s-72e9c65ad0"></a>Profile: `gogurt-cli-human-json/v1`
- <a id="s-3e72d23db1"></a>Structured output: `optional-json`
- <a id="s-ea0d4f4135"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-c5cc1a53c6"></a>`marker-preview` | <a id="s-6c131d3c1d"></a>`{"kind":"option-equals","parameter":"dry_run","value":true}` | <a id="s-f85e137cbe"></a>`0` | <a id="s-2a98a7b465"></a>`human: noncontractual-presentation-of-command-result; json: gogurt-marker-write-plan/v1` | <a id="s-d5c25e107a"></a>`all: empty` |
| <a id="s-65376fa652"></a>`marker-published` | <a id="s-1ace6f4a30"></a>`{"kind":"option-equals","parameter":"dry_run","value":false}` | <a id="s-2bdaa45e78"></a>`0` | <a id="s-7ff22340dc"></a>`human: noncontractual-presentation-of-command-result; json: gogurt-marker-publication/v1` | <a id="s-258261044f"></a>`all: empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-a116aaabaf"></a>`usage` | <a id="s-7f4e752285"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-eee78944c6"></a>`2` | <a id="s-759246dcbb"></a>`all: empty` | <a id="s-1ad8401f8f"></a>`all: noncontractual-usage-diagnostic` |
| <a id="s-d95fc38e29"></a>`operational` | <a id="s-36d3949039"></a>`{"kind":"application-error"}` | <a id="s-8821274e2c"></a>`1` | <a id="s-d2bc22b47c"></a>`human: empty; json: gogurt-cli-error/v1` | <a id="s-86733215f2"></a>`human: noncontractual-diagnostic; json: empty` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --config](#s-f0b9492d8e) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --dry-run](#s-cef35a84fe) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --force](#s-aba7dbf38e) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-d2fbb045b4) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter mount_point](#s-08581ea9b7) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --mounted-volume-provider](#s-5d3f067691) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter route](#s-e3296e8a22) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-2f65838f53"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-6ba375e2b7"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources.md#src-3b2297c37d) — `reference/gogurt/application/src/gogurt/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/gogurt/commands/write/name`
- `/external_contract/cli/gogurt/commands/write/parameters`
- `/external_contract/cli/gogurt/commands/write/result_contract`
- `/external_contract/cli/gogurt/commands/write/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/write/name`

<!-- exact-contract-value: bc2434a12fecfa191ab66b56a67f8fe2507216b76be96a72b1f866562cdc3567 -->

```json
"write"
```

### `/external_contract/cli/gogurt/commands/write/parameters`

<!-- exact-contract-value: b3d48e6509916c8278252ee2275d02848c9d2f69f23af502a7b82e22f6097f49 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "route",
    "nargs": 1,
    "options": [
      "route"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  },
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "mount_point",
    "nargs": 1,
    "options": [
      "mount_point"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer.models.TyperPath",
      "name": "path"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "config",
    "nargs": 1,
    "options": [
      "--config"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer.models.TyperPath",
      "name": "path"
    }
  },
  {
    "count": false,
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "force",
    "nargs": 1,
    "options": [
      "--force"
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
    "name": "dry_run",
    "nargs": 1,
    "options": [
      "--dry-run"
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
  },
  {
    "count": false,
    "envvar": "GOGURT_MOUNTED_VOLUME_PROVIDER",
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "mounted_volume_provider",
    "nargs": 1,
    "options": [
      "--mounted-volume-provider"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  }
]
```

### `/external_contract/cli/gogurt/commands/write/result_contract`

<!-- exact-contract-value: 46ec87e4bfda84fe647bf9fe29b08983dec9a4c1e23d7a9f81c1b5010934a88e -->

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
  "identity": "gogurt-cli-result/write/v1",
  "profile_id": "gogurt-cli-human-json/v1",
  "structured_output": "optional-json",
  "success": [
    {
      "exit_status": 0,
      "id": "marker-preview",
      "selected_by": {
        "kind": "option-equals",
        "parameter": "dry_run",
        "value": true
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": {
          "identity": "gogurt-marker-write-plan/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "type": "object"
          }
        }
      }
    },
    {
      "exit_status": 0,
      "id": "marker-published",
      "selected_by": {
        "kind": "option-equals",
        "parameter": "dry_run",
        "value": false
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": {
          "identity": "gogurt-marker-publication/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "marker": {
                "type": "object"
              },
              "marker_identity": {
                "type": "string"
              },
              "mount_point": {
                "type": "string"
              },
              "mounted_volume_provider": {
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
              }
            },
            "required": [
              "mount_point",
              "mounted_volume_provider",
              "marker",
              "marker_identity"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/gogurt/commands/write/terminating_controls`

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
