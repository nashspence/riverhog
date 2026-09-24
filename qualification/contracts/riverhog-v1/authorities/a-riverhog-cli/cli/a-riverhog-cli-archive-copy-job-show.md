# a-riverhog-cli archive copy-job show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-archive-copy-job-show:f1b1f09740 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-fe864f3b92"></a>Parser name: `show`
- <a id="s-078f62f7fe"></a>Extra arguments at this parser: rejected.
- <a id="s-b410c905d6"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-4d995bd987"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-06d236f4e7"></a>`selector`<br>`selector` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-ad0ae21409"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-5dc4b03914"></a>`help` | <a id="s-0694838bbf"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-154ef2c35a"></a>`0` | <a id="s-b2315b2e98"></a>`"noncontractual-framework-help"` | <a id="s-3db3e0cf68"></a>`"empty"` |

### Result and failure contract

- <a id="s-e87d2cead5"></a>Result identity: `a-riverhog-cli-result/archive/copy-job/show/v1`
- <a id="s-0bf71c3e5b"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-ef80cf1838"></a>Structured output: `optional-json`
- <a id="s-39d2e8ab11"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-f14ee5762d"></a>`completed` | <a id="s-b1b7606795"></a>`{"kind":"command-completed"}` | <a id="s-aa3b46350e"></a>`0` | <a id="s-7fabe93b59"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP get_archive_copy_job response 200](../../riverhog/http-operations/get-v1-archive-copy-jobs-collection-id-destination-store.md#s-0ec025bfd6) | <a id="s-516141ccd1"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-2b9255d50a"></a>`usage` | <a id="s-461c099c83"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-e95095ee21"></a>`2` | <a id="s-353aed05a7"></a>all: `"empty"` | <a id="s-e18e5d5f79"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-e8db05a48b"></a>`operational` | <a id="s-2a665bdce1"></a>`{"kind":"application-error"}` | <a id="s-054b577fec"></a>`1` | <a id="s-6e6c10e422"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-3d0caf3234"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-ad0ae21409) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter selector](#s-06d236f4e7) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/archive/copy-jobs/{collection_id}/{destination_store}](../../riverhog/http-operations/get-v1-archive-copy-jobs-collection-id-destination-store.md)
- [riverhog_client.ApiClient.get_archive_copy_job](../../riverhog-client/python/riverhog-client-apiclient-get-archive-copy-job.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-e691acdd17"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-a6007277bf"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::archive\_copy\_show\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L3059)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/show/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/show/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/show/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/show/name`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/show/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/show/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/show/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/show/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/show/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/show/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/show/parameters`

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

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/show/result_contract`

<!-- exact-contract-value: 21a38c69a62110df239efbf4e6d86fa3f65e738c697745c43e6b04442be48f80 -->

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
  "identity": "a-riverhog-cli-result/archive/copy-job/show/v1",
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
          "operation_id": "get_archive_copy_job",
          "path": "/v1/archive/copy-jobs/{collection_id}/{destination_store}",
          "schema": {
            "$ref": "#/components/schemas/ArchiveCopyJobOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-cli/commands/archive/commands/copy-job/commands/show/terminating_controls`

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
