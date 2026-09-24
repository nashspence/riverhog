# a-riverhog-cli app key revoke

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-app-key-revoke:45d181c4a7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-fde0b8f0e5"></a>Parser name: `revoke`
- <a id="s-7215d5ce1e"></a>Extra arguments at this parser: rejected.
- <a id="s-b590819e21"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-0afba29ab6"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-9eb109ee62"></a>`app_name`<br>`app_name` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-3084684388"></a>`key_id`<br>`key_id` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-ca3964b7ea"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-a91727bb95"></a>`help` | <a id="s-e620a70146"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-c629679606"></a>`0` | <a id="s-88d7e7c785"></a>`"noncontractual-framework-help"` | <a id="s-966fa66af8"></a>`"empty"` |

### Result and failure contract

- <a id="s-1854b6d570"></a>Result identity: `a-riverhog-cli-result/app/key/revoke/v1`
- <a id="s-f2252701b8"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-56bf45d669"></a>Structured output: `optional-json`
- <a id="s-a3629377f8"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-28a222999a"></a>`completed` | <a id="s-fbb7939ae3"></a>`{"kind":"command-completed"}` | <a id="s-80566f6d58"></a>`0` | <a id="s-4b3f9ca1a7"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP revoke_app_key response 200](../../riverhog/http-operations/post-v1-apps-app-keys-key-id-revoke.md#s-bb1d20bb4c) | <a id="s-e61f3ae98e"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-f9b28926e0"></a>`usage` | <a id="s-74343845e4"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-b14e6c2693"></a>`2` | <a id="s-6e1145344c"></a>all: `"empty"` | <a id="s-f0e18695c0"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-5766e3aab6"></a>`operational` | <a id="s-9d989327fe"></a>`{"kind":"application-error"}` | <a id="s-6a4029dfac"></a>`1` | <a id="s-ecc17e2983"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-55573a8883"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter app_name](#s-9eb109ee62) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-ca3964b7ea) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter key_id](#s-3084684388) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [POST /v1/apps/{app}/keys/{key_id}/revoke](../../riverhog/http-operations/post-v1-apps-app-keys-key-id-revoke.md)
- [riverhog_client.ApiClient.revoke_app_key](../../riverhog-client/python/riverhog-client-apiclient-revoke-app-key.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-a05d802279"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-d2f8ed17f8"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::app\_key\_revoke\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L1211)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/revoke/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/revoke/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/revoke/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/revoke/name`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/revoke/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/revoke/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/revoke/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/revoke/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/revoke/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/revoke/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/revoke/name`

<!-- exact-contract-value: 4dbb5115705a0de2a6127443c67e751a0baa58326bad39f890b1efc796cc6a3a -->

```json
"revoke"
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/revoke/parameters`

<!-- exact-contract-value: 1749f130449a17b41843bc114da325f40e7d6ade60854043985e5bcec11d2cad -->

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

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/revoke/result_contract`

<!-- exact-contract-value: 8ebaa8a67cecc1c436a3600f22fee6cf6ec235176fe5a58156d1bb35c2b2380f -->

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
  "identity": "a-riverhog-cli-result/app/key/revoke/v1",
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
          "operation_id": "revoke_app_key",
          "path": "/v1/apps/{app}/keys/{key_id}/revoke",
          "schema": {
            "$ref": "#/components/schemas/AppKeyOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/revoke/terminating_controls`

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
