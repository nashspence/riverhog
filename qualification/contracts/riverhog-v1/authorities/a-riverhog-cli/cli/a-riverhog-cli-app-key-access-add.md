# a-riverhog-cli app key access add

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-app-key-access-add:8cef780362 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-ffd582cd58"></a>Parser name: `add`
- <a id="s-59056b428f"></a>Extra arguments at this parser: rejected.
- <a id="s-acafa6053b"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-508eee40b0"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-16fddb39bc"></a>`app_name`<br>`app_name` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-1abd733f00"></a>`key_id`<br>`key_id` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-92e13060b4"></a>`allow`<br>`allow` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-cb632a98f0"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-22702df3c8"></a>`help` | <a id="s-c3dfdbc6dc"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-2d9286136f"></a>`0` | <a id="s-9e4663818e"></a>`"noncontractual-framework-help"` | <a id="s-d213dcf90f"></a>`"empty"` |

### Result and failure contract

- <a id="s-1368e3e2a1"></a>Result identity: `a-riverhog-cli-result/app/key/access/add/v1`
- <a id="s-131b722d75"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-0828b01a6b"></a>Structured output: `optional-json`
- <a id="s-bbaa3b6d66"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-22bd3b914c"></a>`completed` | <a id="s-6443a5ade4"></a>`{"kind":"command-completed"}` | <a id="s-11e44bfdb9"></a>`0` | <a id="s-3f17988d12"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP add_app_key_access response 200](../../riverhog/http-operations/post-v1-apps-app-keys-key-id-access.md#s-f1906aba47) | <a id="s-1646743418"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-36ada490ec"></a>`usage` | <a id="s-739e75d86d"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-0cde76ef3e"></a>`2` | <a id="s-d8d6297470"></a>all: `"empty"` | <a id="s-1b84e3d5d0"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-ac731bddf0"></a>`operational` | <a id="s-b62c74ad52"></a>`{"kind":"application-error"}` | <a id="s-d0cad82b99"></a>`1` | <a id="s-383b7a5529"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-08a12e62d2"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter allow](#s-92e13060b4) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter app_name](#s-16fddb39bc) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-cb632a98f0) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter key_id](#s-1abd733f00) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [POST /v1/apps/{app}/keys/{key_id}/access](../../riverhog/http-operations/post-v1-apps-app-keys-key-id-access.md)
- [riverhog_client.ApiClient.add_app_key_access](../../riverhog-client/python/riverhog-client-apiclient-add-app-key-access.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-9bbd093d2b"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-f721fbde13"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::app\_key\_access\_add\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L1318)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/add/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/add/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/add/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/add/name`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/add/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/add/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/add/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/add/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/add/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/add/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/add/name`

<!-- exact-contract-value: 7b8a6f33b43ca26a3f2aa73e408748f9ceb391ac21dfe746c94563016ab72f85 -->

```json
"add"
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/add/parameters`

<!-- exact-contract-value: f5e3dd9d1b01d343a526cfe2ef1b0146c4123c114321a56ec7da1a3143daace5 -->

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
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "allow",
    "nargs": 1,
    "options": [
      "allow"
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

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/add/result_contract`

<!-- exact-contract-value: de3b630c25641c9777fa0a980e805522dc1807847495b9f725c6d78fb64bd5c4 -->

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
  "identity": "a-riverhog-cli-result/app/key/access/add/v1",
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
          "operation_id": "add_app_key_access",
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

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/add/terminating_controls`

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
