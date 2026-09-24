# a-riverhog-cli collection upload show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-collection-upload-show:6c8655183a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-66a08a1ff5"></a>Parser name: `show`
- <a id="s-10bb971942"></a>Extra arguments at this parser: rejected.
- <a id="s-b89a09c000"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-a2d562ff34"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-e5d3c4795d"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-52f6d80154"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-191505e29d"></a>`help` | <a id="s-2db4df76e1"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-02a55dedd2"></a>`0` | <a id="s-06cbddd35f"></a>`"noncontractual-framework-help"` | <a id="s-ac86d703fc"></a>`"empty"` |

### Result and failure contract

- <a id="s-9fd45c0014"></a>Result identity: `a-riverhog-cli-result/collection/upload/show/v1`
- <a id="s-adba0fbc3c"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-adb31a12c9"></a>Structured output: `optional-json`
- <a id="s-c38d714341"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-0dc10adce5"></a>`completed` | <a id="s-5c10081c3f"></a>`{"kind":"command-completed"}` | <a id="s-2ae1a6dce1"></a>`0` | <a id="s-c380c2c2e0"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP get_collection_upload_session response 200](../../riverhog/http-operations/get-v1-collection-upload-sessions-collection-id.md#s-3478a57d08) | <a id="s-cf539fab88"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-819cedb6e7"></a>`usage` | <a id="s-ac5a3bd8d6"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-c79d513cc1"></a>`2` | <a id="s-4fe2db62fa"></a>all: `"empty"` | <a id="s-00052dcc60"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-db8a902b3e"></a>`operational` | <a id="s-a0a59ae52c"></a>`{"kind":"application-error"}` | <a id="s-2528447f66"></a>`1` | <a id="s-3a385e5a21"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-c49f5b44ff"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-e5d3c4795d) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-52f6d80154) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collection-upload-sessions/{collection_id}](../../riverhog/http-operations/get-v1-collection-upload-sessions-collection-id.md)
- [riverhog_client.ApiClient.get_collection_upload_session](../../riverhog-client/python/riverhog-client-apiclient-get-collection-upload-session.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-a8b916f674"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-f2addf01ac"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::upload\_show\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L2403)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/show/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/show/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/show/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/show/name`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/show/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/show/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/show/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/show/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/show/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/show/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/show/parameters`

<!-- exact-contract-value: 69121b7dd4df39852c314f302ca34fb358e3d4472f9e5565bdec50242e30ee3a -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "collection_id",
    "nargs": 1,
    "options": [
      "collection_id"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.IntParamType",
      "name": "integer"
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

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/show/result_contract`

<!-- exact-contract-value: 646de6e3ad5011c977794c8816192f2c44d40184f356bd70ac14ef61ddfad09f -->

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
  "identity": "a-riverhog-cli-result/collection/upload/show/v1",
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
          "method": "GET",
          "operation_id": "get_collection_upload_session",
          "path": "/v1/collection-upload-sessions/{collection_id}",
          "schema": {
            "$ref": "#/components/schemas/CollectionUploadSessionOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/upload/commands/show/terminating_controls`

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
