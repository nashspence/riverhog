# a-riverhog-cli collection provenance verification-show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-collection-provenance-veri-15cc736d46:96b7047dd3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-be1d517624"></a>Parser name: `verification-show`
- <a id="s-1f3283594d"></a>Extra arguments at this parser: rejected.
- <a id="s-21d4e20270"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-c7848e427a"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-731506bc16"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-0557b69759"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-a7911d49b5"></a>`help` | <a id="s-5eae483ac7"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-514a203f8f"></a>`0` | <a id="s-9c59b3003f"></a>`"noncontractual-framework-help"` | <a id="s-a5d254db85"></a>`"empty"` |

### Result and failure contract

- <a id="s-5bc4c15f8b"></a>Result identity: `a-riverhog-cli-result/collection/provenance/verification-show/v1`
- <a id="s-184c7d6fb5"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-4eb454a5b6"></a>Structured output: `optional-json`
- <a id="s-71cbd5a103"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-08d6c1767e"></a>`completed` | <a id="s-c15e60a95b"></a>`{"kind":"command-completed"}` | <a id="s-fd98c17a1c"></a>`0` | <a id="s-0b609adb60"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP get_collection_provenance_verification response 200](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-verification.md#s-d1f90bc6b1) | <a id="s-13bddbcb7b"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-5bedff278c"></a>`usage` | <a id="s-2c23775986"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-385f45231d"></a>`2` | <a id="s-26678eb305"></a>all: `"empty"` | <a id="s-83e38a2682"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-24b538489d"></a>`operational` | <a id="s-ed9963ca16"></a>`{"kind":"application-error"}` | <a id="s-eb1cda0ba4"></a>`1` | <a id="s-0d65e21bd6"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-3f92729f3b"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-731506bc16) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-0557b69759) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/provenance/verification](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-verification.md)
- [riverhog_client.ApiClient.get_collection_provenance_verification](../../riverhog-client/python/riverhog-client-apiclient-get-collection-provenance-verification.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-11b679826b"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-4c39c2341e"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::provenance\_verification\_show\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L2764)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verification-show/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verification-show/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verification-show/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verification-show/name`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verification-show/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verification-show/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verification-show/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verification-show/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verification-show/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verification-show/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verification-show/name`

<!-- exact-contract-value: 2ad855165a84772f590f6bdd471c8bce64b16730e7186147c2b558c1f38974da -->

```json
"verification-show"
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verification-show/parameters`

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

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verification-show/result_contract`

<!-- exact-contract-value: 7e860c2bf47576a34e59f3864c437a613e1eee02126a3db0067e73e4da6a260c -->

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
  "identity": "a-riverhog-cli-result/collection/provenance/verification-show/v1",
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
          "operation_id": "get_collection_provenance_verification",
          "path": "/v1/collections/{collection_id}/provenance/verification",
          "schema": {
            "$ref": "#/components/schemas/CollectionProvenanceVerificationJobOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/provenance/commands/verification-show/terminating_controls`

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
