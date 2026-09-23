# a-riverhog-cli app key access remove

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-app-key-access-remove:a58bbc98f4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-ef3e07c827"></a>Parser name: `remove`
- <a id="s-f6659da7ba"></a>Extra arguments at this parser: rejected.
- <a id="s-0ce5745a49"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-f6a29c8ae2"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-3eb78dea5a"></a>`selector`<br>`selector` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-8260b140bf"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-a4ad7f469d"></a>`help` | <a id="s-34063fa3fb"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-7a35330d25"></a>`0` | <a id="s-730b8b67c7"></a>`"noncontractual-framework-help"` | <a id="s-a7ca0bceaa"></a>`"empty"` |

### Result and failure contract

- <a id="s-c5f1bea1e9"></a>Result identity: `a-riverhog-cli-result/app/key/access/remove/v1`
- <a id="s-f52058bb3d"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-8f5acd8a1a"></a>Structured output: `optional-json`
- <a id="s-a8029cd66f"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-ca48fee9fa"></a>`completed` | <a id="s-729eb0e0fc"></a>`{"kind":"command-completed"}` | <a id="s-c7cbe31c68"></a>`0` | <a id="s-59e0f4c491"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP remove_app_key_access response 200](../../riverhog/http-operations/delete-v1-apps-app-keys-key-id-access.md#s-80b8e88010) | <a id="s-15c7cbdeed"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-97bdb94dcb"></a>`usage` | <a id="s-0cc8caf6ca"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-19f73a0411"></a>`2` | <a id="s-43ac4f0bd0"></a>all: `"empty"` | <a id="s-7eaaf293e5"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-cbeee29584"></a>`operational` | <a id="s-cae3c14668"></a>`{"kind":"application-error"}` | <a id="s-583cab7e19"></a>`1` | <a id="s-f1b1d8dded"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-d582b7644b"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-8260b140bf) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter selector](#s-3eb78dea5a) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [DELETE /v1/apps/{app}/keys/{key_id}/access](../../riverhog/http-operations/delete-v1-apps-app-keys-key-id-access.md)
- [riverhog_client.ApiClient.remove_app_key_access](../../riverhog-client/python/riverhog-client-apiclient-remove-app-key-access.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-d0ddd1f2bb"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-c21283e949"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::app\_key\_access\_remove\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L1337)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/remove/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/remove/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/remove/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/remove/name`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/remove/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/remove/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/remove/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/remove/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/remove/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/remove/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/remove/name`

<!-- exact-contract-value: 66f3d68b7627c9f6965029d01923daefa869b3f33123c145ddb25464b41a529a -->

```json
"remove"
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/remove/parameters`

<!-- exact-contract-value: 6b32bc56c11c2cdafb747d7a7aa740468c2f614b5b1d0e2588fb031d746e52f9 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "selector",
    "nargs": 1,
    "options": [
      "selector"
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

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/remove/result_contract`

<!-- exact-contract-value: c14b6e83214f46c867f2253fb86309e7b8c34d8d265b0c5fec55bb859fdfed70 -->

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
  "identity": "a-riverhog-cli-result/app/key/access/remove/v1",
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
          "method": "DELETE",
          "operation_id": "remove_app_key_access",
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

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/access/commands/remove/terminating_controls`

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
