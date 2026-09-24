# a-riverhog-cli collection tag contains

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-collection-tag-contains:e514f00871 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-5ff5b29598"></a>Parser name: `contains`
- <a id="s-de165d4d63"></a>Extra arguments at this parser: rejected.
- <a id="s-02a7dca70b"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-fcec925d66"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-e25b9672c2"></a>`collection_id`<br>`collection_id` | required positional; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-0356b107e2"></a>`tag`<br>`tag` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-141b94ae55"></a>`revision`<br>`--revision` | optional option; 1 value | integer range; minimum=`1` (inclusive); outside range: reject | not recorded<br>Env: `null` |
| <a id="s-eaf4e6cc62"></a>`tag_set_identity`<br>`--tag-set-identity` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-ff177daf87"></a>`json_mode`<br>`--json` | optional flag; 0 values | boolean | `false`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-703715860a"></a>`help` | <a id="s-2c50a2d0cc"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-eba56bd397"></a>`0` | <a id="s-09c191dab6"></a>`"noncontractual-framework-help"` | <a id="s-1ae710315b"></a>`"empty"` |

### Result and failure contract

- <a id="s-e19355d315"></a>Result identity: `a-riverhog-cli-result/collection/tag/contains/v1`
- <a id="s-6fe22ed4c3"></a>Profile: `a-riverhog-cli-human-json/v1`
- <a id="s-5fa5a2f42b"></a>Structured output: `optional-json`
- <a id="s-8ae64c67e9"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-e906da0fac"></a>`completed` | <a id="s-95bd8c8360"></a>`{"kind":"command-completed"}` | <a id="s-9dba8a45f0"></a>`0` | <a id="s-b93769c5a9"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP collection_contains_tag response 200](../../riverhog/http-operations/post-v1-collections-collection-id-tags-contains.md#s-8cf8efb305) | <a id="s-02c12e4f8f"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-abb5afd122"></a>`usage` | <a id="s-5d1af7367e"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-bd49a1b098"></a>`2` | <a id="s-05caae985a"></a>all: `"empty"` | <a id="s-3f0ad4a0b2"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-c5245b9ea1"></a>`operational` | <a id="s-89cf86cc45"></a>`{"kind":"application-error"}` | <a id="s-7402d28728"></a>`1` | <a id="s-5bff6b4a40"></a>human: `"empty"`; json: [http-api-contracts.ErrorOut](../../http-api-contracts/python/http-api-contracts-errorout.md) | <a id="s-b4819b47ce"></a>human: `"noncontractual-diagnostic"`; json: `"empty"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-e25b9672c2) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --json](#s-ff177daf87) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; source_constraint={"field":"is_flag"} |
| [CLI parameter --revision](#s-141b94ae55) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter tag](#s-0356b107e2) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |
| [CLI parameter --tag-set-identity](#s-eaf4e6cc62) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}](../../riverhog/http-operations/get-v1-collections-collection-id.md)
- [POST /v1/collections/{collection_id}/tags:contains](../../riverhog/http-operations/post-v1-collections-collection-id-tags-contains.md)
- [riverhog_client.ApiClient.collection_contains_tag](../../riverhog-client/python/riverhog-client-apiclient-collection-contains-tag.md)
- [riverhog_client.ApiClient.get_collection](../../riverhog-client/python/riverhog-client-apiclient-get-collection.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-1adae63136"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-c1008bec5c"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::collection\_tag\_contains\_cmd](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py#L1032)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/contains/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/contains/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/contains/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/contains/name`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/contains/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/contains/result_contract`
- `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/contains/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/contains/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/contains/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/contains/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/contains/name`

<!-- exact-contract-value: dde0e3dc0ad9a0daf42c9d0ab5a9a1c466093b12ccae1ca4c613754a34526930 -->

```json
"contains"
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/contains/parameters`

<!-- exact-contract-value: 545f7a270aa271b951b2b6ce4d0313284cd9758e6589f90cc348b4de22d965e2 -->

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
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "tag",
    "nargs": 1,
    "options": [
      "tag"
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
    "multiple": false,
    "name": "revision",
    "nargs": 1,
    "options": [
      "--revision"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "clamp": false,
      "class": "typer._click.types.IntRange",
      "max_open": false,
      "min_open": false,
      "minimum": 1,
      "name": "integer range"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "tag_set_identity",
    "nargs": 1,
    "options": [
      "--tag-set-identity"
    ],
    "required": false,
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

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/contains/result_contract`

<!-- exact-contract-value: 9169bff4ef56593ba838f7822fd2934460f1a227a81a7fa3efe4105796f7b791 -->

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
  "identity": "a-riverhog-cli-result/collection/tag/contains/v1",
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
          "operation_id": "collection_contains_tag",
          "path": "/v1/collections/{collection_id}/tags:contains",
          "schema": {
            "$ref": "#/components/schemas/CollectionTagMembershipOut"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/commands/tag/commands/contains/terminating_controls`

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
