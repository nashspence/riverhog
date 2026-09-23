# a-riverhog-cli app key create

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-app-key-create:7c3ad55269 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-be3830b7b9"></a>Parser name: `create`
- <a id="s-dc720cf04a"></a>Extra arguments at this parser: rejected.
- <a id="s-488f86b6df"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-2bd3ac631c"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-5a55add3c4"></a>`app_name`<br>`app_name` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-93108955be"></a>`allow`<br>`--allow` | required option; 1 value; collects repeats; no declared occurrence maximum | text | not recorded<br>Env: `null` |
| <a id="s-f243538db4"></a>`expires_in`<br>`--expires-in` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-0fd4a26b17"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-4f7a660ad8"></a>`help` | <a id="s-1e91a159aa"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-ba1d10e3fb"></a>`0` | <a id="s-b781a120b0"></a>`"noncontractual-framework-help"` | <a id="s-ebaa8445da"></a>`"empty"` |

### Result and failure contract

- <a id="s-da833bec54"></a>Result identity: `a-riverhog-cli-result/app/key/create/v1`
- <a id="s-72e4f10702"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-4aff6af8a6"></a>Structured output: `optional-json`
- <a id="s-199b1ef918"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-92987fa7ca"></a>`completed` | <a id="s-f7106afda5"></a>`{"kind":"command-completed"}` | <a id="s-4624054170"></a>`0` | <a id="s-07c3d4f97b"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP create_app_key response 200](../../riverhog/http-operations/post-v1-apps-app-keys.md#s-642e16127e) | <a id="s-2beec3718f"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-aaf1e8caff"></a>`usage` | <a id="s-c5c5df30c2"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-2722416d28"></a>`2` | <a id="s-303387abee"></a>all: `"empty"` | <a id="s-73a2628894"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-7b746c07e3"></a>`operational` | <a id="s-74a3935d93"></a>`{"kind":"application-error"}` | <a id="s-22a3d271f2"></a>`1` | <a id="s-5ddef711e6"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-805d8f7e48"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"a-riverhog-cli"}; maximum=null; reason="no-declared-semantic-maximum"; source_constraint={"field":"multiple"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --allow](#s-93108955be) | `cardinality · occurrences · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --allow](#s-93108955be) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter app_name](#s-5a55add3c4) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --expires-in](#s-f243538db4) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-0fd4a26b17) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Maintained corroboration

### Related interface records

- [POST /v1/apps/{app}/keys](../../riverhog/http-operations/post-v1-apps-app-keys.md)
- [riverhog_client.ApiClient.create_app_key](../../riverhog-client/python/riverhog-client-apiclient-create-app-key.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-5425fe7d4e"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-e079551311"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-9b92338027"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::app\_key\_create\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L1135)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/create/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/create/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/create/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/create/name`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/create/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/create/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/create/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/create/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/create/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/create/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/create/name`

<!-- exact-contract-value: 5498a731a187f424a5800943afcba027f3a6cd684e38fe6e40c02bee1753152d -->

```json
"create"
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/create/parameters`

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

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/create/result_contract`

<!-- exact-contract-value: 8119195326613668084f9c46909756b3acbc59d3b8cfdf5a7b30f2781c67cabd -->

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
  "identity": "a-riverhog-cli-result/app/key/create/v1",
  "profile_id": "a-riverhog-cli-human-json/v1",
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

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/create/terminating_controls`

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

</details>
