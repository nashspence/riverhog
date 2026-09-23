# a-riverhog-cli local provenance-observer show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-local-provenance-observer-show:3ed9a553ec -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-f07b5f47c4"></a>Parser name: `show`
- <a id="s-57872277fa"></a>Extra arguments at this parser: rejected.
- <a id="s-4675787dae"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-12d51319f0"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-5efa38b191"></a>`name`<br>`name` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-4b7d9ba637"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-4f4b1ed97e"></a>`help` | <a id="s-b11165e2b8"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-0fc3418067"></a>`0` | <a id="s-533dcc89c5"></a>`"noncontractual-framework-help"` | <a id="s-9419ca5a08"></a>`"empty"` |

### Result and failure contract

- <a id="s-44c13b6523"></a>Result identity: `a-riverhog-cli-result/local/provenance-observer/show/v1`
- <a id="s-e62c93e508"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-6a2f7421eb"></a>Structured output: `optional-json`
- <a id="s-193f97be5a"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-1bcc56a32d"></a>`completed` | <a id="s-eb7e99e751"></a>`{"kind":"command-completed"}` | <a id="s-4f8ec3879d"></a>`0` | <a id="s-24e12371b3"></a>human: `"noncontractual-presentation-of-command-result"`; json: [riverhog-provenance-observer-binding/v1](#s-c427a3a2a5) | <a id="s-8acb6d5b10"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-4f40a05da1"></a>`usage` | <a id="s-77b70a5f8f"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-d6537ac7f5"></a>`2` | <a id="s-c09d722d15"></a>all: `"empty"` | <a id="s-f19246fb66"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-8e0b22ff86"></a>`operational` | <a id="s-d87908f46f"></a>`{"kind":"application-error"}` | <a id="s-97d9a9e2a4"></a>`1` | <a id="s-430a9f09b4"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-859339964a"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-c427a3a2a5"></a>`riverhog-provenance-observer-binding/v1`

Applies to: completed · stdout (json).

<a id="s-a87836781c"></a>

- <a id="s-9c65b78c8e"></a>`type`: `"object"`
- <a id="s-1856e3797d"></a>`required`: `["format","name","observer_id","contract_provider","contract_id","contract_sha256","schema_dialect","format_policy","schema_ids"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-29e780348f"></a>`format` | yes | const="riverhog-provenance-observer-binding/v1" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-4b7d9ba637) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter name](#s-5efa38b191) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-0f6147b52d"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-646b6e45c7"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/show/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/show/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/show/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/show/name`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/show/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/show/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/show/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/show/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/show/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/show/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/show/parameters`

<!-- exact-contract-value: 5fc7dfae38d65070ca7b7f9d59934174307d6a3212e66d1af82fe71c64370451 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "name",
    "nargs": 1,
    "options": [
      "name"
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

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/show/result_contract`

<!-- exact-contract-value: b7e8482ddd593c7add362c09bcdb7b15c7f3f92a2a1b2594d4b89cc72e363066 -->

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
  "identity": "a-riverhog-cli-result/local/provenance-observer/show/v1",
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
          "identity": "riverhog-provenance-observer-binding/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "properties": {
              "format": {
                "const": "riverhog-provenance-observer-binding/v1"
              }
            },
            "required": [
              "format",
              "name",
              "observer_id",
              "contract_provider",
              "contract_id",
              "contract_sha256",
              "schema_dialect",
              "format_policy",
              "schema_ids"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/provenance-observer/commands/show/terminating_controls`

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
