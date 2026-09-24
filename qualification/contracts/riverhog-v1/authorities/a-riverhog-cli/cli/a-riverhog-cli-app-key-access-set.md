# a-riverhog-cli app key access set

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-app-key-access-set:536612efcd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-7fde3fb107"></a>Parser name: `set`
- <a id="s-a727ae36dd"></a>Extra arguments at this parser: rejected.
- <a id="s-d5273f0a0c"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-cd535e4163"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-85690cd895"></a>`app_name`<br>`app_name` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-0d17f1748f"></a>`key_id`<br>`key_id` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-b0ad88bd59"></a>`allow`<br>`--allow` | required option; 1 value; collects repeats; no declared occurrence maximum | text | not recorded<br>Env: `null` |
| <a id="s-738b69290c"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-644086eeab"></a>`help` | <a id="s-4b95ba2b13"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-837cc52495"></a>`0` | <a id="s-78f285ae77"></a>`"noncontractual-framework-help"` | <a id="s-f22eb47eaf"></a>`"empty"` |

### Result and failure contract

- <a id="s-d960180237"></a>Result identity: `a-riverhog-cli-result/app/key/access/set/v1`
- <a id="s-d3ee41670a"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-a6bf1cb1cb"></a>Structured output: `optional-json`
- <a id="s-2910f65707"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-2a7d78a6af"></a>`completed` | <a id="s-4ff5a8961d"></a>`{"kind":"command-completed"}` | <a id="s-62bdaf5acb"></a>`0` | <a id="s-5410e376f1"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP replace_app_key_access response 200](../../riverhog/http-operations/put-v1-apps-app-keys-key-id-access.md#s-6ffd4b8519) | <a id="s-3e8e875ce1"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-c925f6ad62"></a>`usage` | <a id="s-a816e871e8"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-eabf41bd88"></a>`2` | <a id="s-43026a244a"></a>all: `"empty"` | <a id="s-e831bce02f"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-08a87be868"></a>`operational` | <a id="s-cf00164ddd"></a>`{"kind":"application-error"}` | <a id="s-c4a0364997"></a>`1` | <a id="s-50c79d2725"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-89fdcb4a4e"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"a-riverhog-cli"}; maximum=null; reason="no-declared-semantic-maximum"; source_constraint={"field":"multiple"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --allow](#s-b0ad88bd59) | `cardinality · occurrences · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --allow](#s-b0ad88bd59) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter app_name](#s-85690cd895) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-738b69290c) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter key_id](#s-0d17f1748f) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [PUT /v1/apps/{app}/keys/{key_id}/access](../../riverhog/http-operations/put-v1-apps-app-keys-key-id-access.md)
- [riverhog_client.ApiClient.replace_app_key_access](../../riverhog-client/python/riverhog-client-apiclient-replace-app-key-access.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-5bd7644302"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-d22eafe3e9"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-ad8de190c0"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::app\_key\_access\_set\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L1295)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/set/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/set/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/set/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/set/name`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/set/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/set/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/set/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/set/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/set/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/set/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/set/name`

<!-- exact-contract-value: c7f5814b92ec9430648136406d844823df66e1af010dfa9456b5ec7ff5017f8b -->

```json
"set"
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/set/parameters`

<!-- exact-contract-value: cdcd4dd6dee20d048c729bade152629ffeb7fdd74ef855d37167453f2384431b -->

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
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "key_id",
    "nargs": 1,
    "options": [
      "key_id"
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

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/set/result_contract`

<!-- exact-contract-value: 02dcc539e57940943e129ca58a185fea13b34ee64b0dc4e6e24556f656528a9f -->

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
          "identity": "http-api-contracts.ErrorOut",
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
            "title": "ErrorOut",
            "type": "object"
          }
        }
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "a-riverhog-cli-result/app/key/access/set/v1",
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
          "method": "PUT",
          "operation_id": "replace_app_key_access",
          "path": "/v1/apps/{app}/keys/{key_id}/access",
          "schema": {
            "$ref": "#/components/schemas/AppAccessSetOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/set/terminating_controls`

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
