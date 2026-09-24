# a-riverhog-cli app key quota set

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-app-key-quota-set:b2620adf61 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-43bee83bc4"></a>Parser name: `set`
- <a id="s-b75832517f"></a>Extra arguments at this parser: rejected.
- <a id="s-1cb00fdb96"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-37776c6c80"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-efe4edcbcf"></a>`app_name`<br>`app_name` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-b6acca92d5"></a>`key_id`<br>`key_id` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-e0d584bd1b"></a>`limit`<br>`limit` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-6ac104ea26"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-0d6483bf29"></a>`help` | <a id="s-e4b3e17f47"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-7d24221230"></a>`0` | <a id="s-37c5f60326"></a>`"noncontractual-framework-help"` | <a id="s-f344da2679"></a>`"empty"` |

### Result and failure contract

- <a id="s-703f1c25bf"></a>Result identity: `a-riverhog-cli-result/app/key/quota/set/v1`
- <a id="s-24f417c9b5"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-ec3c3ba942"></a>Structured output: `optional-json`
- <a id="s-273908db44"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-511b1ba7df"></a>`completed` | <a id="s-dba6dec7e6"></a>`{"kind":"command-completed"}` | <a id="s-335aa96a43"></a>`0` | <a id="s-e5d4ede0bb"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP set_app_key_download_quota response 200](../../riverhog/http-operations/put-v1-apps-app-keys-key-id-download-quota.md#s-2e6c8a295b) | <a id="s-e233a0f572"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-1ec9e622a4"></a>`usage` | <a id="s-fe435a53b0"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-55bf086532"></a>`2` | <a id="s-e804ecc293"></a>all: `"empty"` | <a id="s-41fd988f9e"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-e94b597e19"></a>`operational` | <a id="s-2f1a6f5441"></a>`{"kind":"application-error"}` | <a id="s-164d8f5d9d"></a>`1` | <a id="s-73a8ae7a5b"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-b287fe308e"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter app_name](#s-efe4edcbcf) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-6ac104ea26) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter key_id](#s-b6acca92d5) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter limit](#s-e0d584bd1b) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [PUT /v1/apps/{app}/keys/{key_id}/download-quota](../../riverhog/http-operations/put-v1-apps-app-keys-key-id-download-quota.md)
- [riverhog_client.ApiClient.set_app_key_download_quota](../../riverhog-client/python/riverhog-client-apiclient-set-app-key-download-quota.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-4336861731"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-5ddfdd8296"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::app\_key\_quota\_set\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L1367)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/set/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/set/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/set/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/set/name`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/set/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/set/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/set/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/set/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/set/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/set/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/set/name`

<!-- exact-contract-value: c7f5814b92ec9430648136406d844823df66e1af010dfa9456b5ec7ff5017f8b -->

```json
"set"
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/set/parameters`

<!-- exact-contract-value: 638090c9b4c8b867084196d21d13a4c6d162923fdd5cb85b06f606947f84e108 -->

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
    "name": "limit",
    "nargs": 1,
    "options": [
      "limit"
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

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/set/result_contract`

<!-- exact-contract-value: 4e471c47779dc382271c58a9870706b04e32c0ecd84b50adf5a4aece37bb9dbb -->

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
  "identity": "a-riverhog-cli-result/app/key/quota/set/v1",
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
          "operation_id": "set_app_key_download_quota",
          "path": "/v1/apps/{app}/keys/{key_id}/download-quota",
          "schema": {
            "$ref": "#/components/schemas/KeyDownloadQuotaOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/commands/set/terminating_controls`

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
