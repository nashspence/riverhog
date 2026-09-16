# piggity app key access set

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-app-key-access-set:112321b199 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-6c89412354"></a>Parser name: `set`
- <a id="s-942a99cbfe"></a>Extra arguments at this parser: rejected.
- <a id="s-07a521a0f6"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-ed0560474c"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-6813df8ce3"></a>`app_name`<br>`app_name` | required positional; 1 value | text | not recorded |
| <a id="s-7d243330c3"></a>`key_id`<br>`key_id` | required positional; 1 value | text | not recorded |
| <a id="s-a9f3d89f96"></a>`allow`<br>`--allow` | required option; 1 value; collects repeats; no declared occurrence maximum | text | not recorded |
| <a id="s-cf6989ab1f"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-73bd73f768"></a>`help` | <a id="s-05fbff705c"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-cdbce52a85"></a>`0` | <a id="s-25f4a1d766"></a>`"noncontractual-framework-help"` | <a id="s-7b8827b401"></a>`"empty"` |

### Result and failure contract

- <a id="s-c6bc19de36"></a>Result identity: `piggity-cli-result/app/key/access/set/v1`
- <a id="s-51afe454a4"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-65cc32c60a"></a>Structured output: `optional-json`
- <a id="s-0b94070d71"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-2ad723399d"></a>`completed` | <a id="s-119abf06d1"></a>`{"kind":"command-completed"}` | <a id="s-d5eb890608"></a>`0` | <a id="s-41841aa36a"></a>human: `noncontractual-presentation-of-command-result`; json: [HTTP replace_app_key_access response 200](../../riverhog/http-operations/put-v1-apps-app-keys-key-id-access.md#s-6ffd4b8519) | <a id="s-482ffc7b8e"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-16e1633e01"></a>`usage` | <a id="s-fbf5363783"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-4a8346c568"></a>`2` | <a id="s-a1740f4685"></a>all: `empty` | <a id="s-3eadabbeec"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-92f3ce2170"></a>`operational` | <a id="s-d46218da16"></a>`{"kind":"application-error"}` | <a id="s-78d4d207fb"></a>`1` | <a id="s-abac293d76"></a>human: `empty`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-d439e6019a"></a>human: `noncontractual-diagnostic`; json: `empty` |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"piggity"}; maximum=null; reason="no-declared-semantic-maximum"; source_constraint={"field":"multiple"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --allow](#s-a9f3d89f96) | `cardinality · occurrences · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --allow](#s-a9f3d89f96) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter app_name](#s-6813df8ce3) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-cf6989ab1f) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter key_id](#s-7d243330c3) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [PUT /v1/apps/{app}/keys/{key_id}/access](../../riverhog/http-operations/put-v1-apps-app-keys-key-id-access.md)
- [riverhog_client.ApiClient.replace_app_key_access](../../riverhog-client/python/riverhog-client-apiclient-replace-app-key-access.md)

## Governing policies

- <a id="pa-9649e3e3ef"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-55226477c9"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-5c9de26678"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/riverhog/applications/piggity/src/piggity/main.py::app_key_access_set_cmd](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py#L1293)

### Machine authority

- `/external_contract/cli/piggity/commands/app/commands/key/commands/access/commands/set/allow_extra_args`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/access/commands/set/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/access/commands/set/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/access/commands/set/name`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/access/commands/set/parameters`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/access/commands/set/result_contract`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/access/commands/set/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/app/commands/key/commands/access/commands/set/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/access/commands/set/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/access/commands/set/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/access/commands/set/name`

<!-- exact-contract-value: c7f5814b92ec9430648136406d844823df66e1af010dfa9456b5ec7ff5017f8b -->

```json
"set"
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/access/commands/set/parameters`

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

### `/external_contract/cli/piggity/commands/app/commands/key/commands/access/commands/set/result_contract`

<!-- exact-contract-value: 9212a19b51b0236c8f4c0acadaea04b78f824d801d6361851cf4abd3c77ab04e -->

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
  "identity": "piggity-cli-result/app/key/access/set/v1",
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

### `/external_contract/cli/piggity/commands/app/commands/key/commands/access/commands/set/terminating_controls`

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
