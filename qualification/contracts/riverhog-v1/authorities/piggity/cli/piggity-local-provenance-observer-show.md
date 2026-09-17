# piggity local provenance-observer show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-local-provenance-observer-show:f054074b07 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-1ba9a6a3ee"></a>Parser name: `show`
- <a id="s-90552f749b"></a>Extra arguments at this parser: rejected.
- <a id="s-ba1c72d583"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-e7b447c6ae"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-6d86b9c512"></a>`name`<br>`name` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-85ac36453b"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-82ab00d372"></a>`help` | <a id="s-07c8ba04b8"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-7ea3bedb5f"></a>`0` | <a id="s-ef9180790a"></a>`"noncontractual-framework-help"` | <a id="s-60f8dbe3bb"></a>`"empty"` |

### Result and failure contract

- <a id="s-899a9a9ce3"></a>Result identity: `piggity-cli-result/local/provenance-observer/show/v1`
- <a id="s-686fd1e75a"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-f353e2b8f0"></a>Structured output: `optional-json`
- <a id="s-a85cd600e0"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-88f645520e"></a>`completed` | <a id="s-e69522cfbc"></a>`{"kind":"command-completed"}` | <a id="s-c62e270b9a"></a>`0` | <a id="s-8d9961d10f"></a>human: `"noncontractual-presentation-of-command-result"`; json: [riverhog-provenance-observer-binding/v1](#s-72f4ebf620) | <a id="s-1bde3632ac"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-ebd0fe5612"></a>`usage` | <a id="s-e988a98490"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-faeed2bedf"></a>`2` | <a id="s-c6abe5a8ff"></a>all: `"empty"` | <a id="s-4514ee0392"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-e7dae9deaf"></a>`operational` | <a id="s-980aa05846"></a>`{"kind":"application-error"}` | <a id="s-eebe915706"></a>`1` | <a id="s-e7371c6eda"></a>human: `"empty"`; json: [http-api-contracts.ErrorResponse](../../http-api-contracts/python/http-api-contracts-errorresponse.md) | <a id="s-ad67867972"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Local structured outputs


#### <a id="s-72f4ebf620"></a>`riverhog-provenance-observer-binding/v1`

Applies to: completed · stdout (json).

<a id="s-970a33669a"></a>

- <a id="s-a66e907a3b"></a>`type`: `"object"`
- <a id="s-ad44d67962"></a>`required`: `["format","name","observer_id","contract_provider","contract_id","contract_sha256","schema_dialect","format_policy","schema_ids"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-de2e570d6e"></a>`format` | yes | const="riverhog-provenance-observer-binding/v1" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-85ac36453b) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter name](#s-6d86b9c512) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Governing policies

- <a id="pa-5bde38f73e"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-c9e3ba5a59"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources/authorities.md#src-094022231f) — [reference/riverhog/applications/piggity/src/piggity/main.py::&lt;module&gt;](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/show/allow_extra_args`
- `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/show/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/show/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/show/name`
- `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/show/parameters`
- `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/show/result_contract`
- `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/show/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/show/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/show/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/show/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/show/parameters`

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

### `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/show/result_contract`

<!-- exact-contract-value: 2f9dfea90128a473fc6f33f6d7ed93cc6cd1a534221759c20a24ec2a8def7b05 -->

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
  "identity": "piggity-cli-result/local/provenance-observer/show/v1",
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

### `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/show/terminating_controls`

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
