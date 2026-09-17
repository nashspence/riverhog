# stove0 recipe validate

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-recipe-validate:cd8639f7a8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-c2446b375b"></a>Parser name: `validate`
- <a id="s-1ff0e95f26"></a>Extra arguments at this parser: rejected.
- <a id="s-ea03e9f305"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-24fa5e6d48"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-b8713f6067"></a>`path`<br>`path` | required positional; 1 value | file; existence required; regular files allowed; directories rejected; access checks on existing paths: read; resolve absolute path and symlinks: no; dash uses normal path checks | not recorded<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-5d3d16ac70"></a>`help` | <a id="s-497325c2c6"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-dfe8d2ca7f"></a>`0` | <a id="s-2eb3f595b2"></a>`"noncontractual-framework-help"` | <a id="s-0a3310add6"></a>`"empty"` |

### Result and failure contract

- <a id="s-25d9608b6e"></a>Result identity: `stove0-cli-result/recipe/validate/v1`
- <a id="s-453cf15582"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-f97c934fa9"></a>Structured output: `optional-json`
- <a id="s-92eade532c"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-c21881a37c"></a>`completed` | <a id="s-20c5e936c6"></a>`{"kind":"command-completed"}` | <a id="s-5dd3e89b2b"></a>`0` | <a id="s-a921e6957c"></a>human: `"noncontractual-presentation-of-command-result"`; json: [stove0-recipe-catalog-validation/v1](#s-a3f0abef56) | <a id="s-6c21a31a21"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-a468e4be4c"></a>`usage` | <a id="s-6bee01f5c8"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-22932551d7"></a>`2` | <a id="s-4562e7c916"></a>all: `"empty"` | <a id="s-1a8131ce7d"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-d9580518c5"></a>`operational` | <a id="s-4170cf0b97"></a>`{"kind":"application-error"}` | <a id="s-cd98d79afb"></a>`1` | <a id="s-c1bc01a565"></a>all: `"empty"` | <a id="s-0d4ee34a0b"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-a3f0abef56"></a>`stove0-recipe-catalog-validation/v1`

Applies to: completed · stdout (json).

<a id="s-79c041b1d3"></a>

- <a id="s-662c5c75ac"></a>`type`: `"object"`
- <a id="s-ea020f6e02"></a>`additionalProperties`: `false`
- <a id="s-cf2dc785f9"></a>`required`: `["format","catalog_sha256","operation_count","recipe_count","recipes"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b847c35f46"></a>`catalog_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-aa05f119a0"></a>`format` | yes | const="stove0-recipe-catalog-validation/v1" |  |
| <a id="s-f787de4709"></a>`operation_count` | yes | type="integer"; minimum=0 |  |
| <a id="s-299b74cea6"></a>`recipe_count` | yes | type="integer"; minimum=0 |  |
| <a id="s-a8b9082a1c"></a>`recipes` | yes | type="array"; items=(type="object") |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter path](#s-b8713f6067) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-76761e57b4"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-16de107f56"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — [reference/stove0/application/client/src/stove0\_cli/main.py::&lt;module&gt;](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/stove0/commands/recipe/commands/validate/allow_extra_args`
- `/external_contract/cli/stove0/commands/recipe/commands/validate/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/recipe/commands/validate/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/recipe/commands/validate/name`
- `/external_contract/cli/stove0/commands/recipe/commands/validate/parameters`
- `/external_contract/cli/stove0/commands/recipe/commands/validate/result_contract`
- `/external_contract/cli/stove0/commands/recipe/commands/validate/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/recipe/commands/validate/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/recipe/commands/validate/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/recipe/commands/validate/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/recipe/commands/validate/name`

<!-- exact-contract-value: 2c9877104bf173f3ecf6b4d44be3e77da3b1b3c87e448be5f6cec01b12ccf804 -->

```json
"validate"
```

### `/external_contract/cli/stove0/commands/recipe/commands/validate/parameters`

<!-- exact-contract-value: cddc1f0f6825983ae34a66405b28385bdb3b2d4c91d146dcb8e46214b06749ce -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "path",
    "nargs": 1,
    "options": [
      "path"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "allow_dash": false,
      "class": "typer.models.TyperPath",
      "dir_okay": false,
      "exists": true,
      "file_okay": true,
      "name": "file",
      "readable": true,
      "resolve_path": false,
      "writable": false
    }
  }
]
```

### `/external_contract/cli/stove0/commands/recipe/commands/validate/result_contract`

<!-- exact-contract-value: 363eb4734df737d7aaf62a2476702b1f8d51814aaeda6154e3f97958d731e9b1 -->

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
        "all": "noncontractual-diagnostic"
      },
      "stdout": {
        "all": "empty"
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "stove0-cli-result/recipe/validate/v1",
  "profile_id": "stove0-cli-human-json/v1",
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
          "identity": "stove0-recipe-catalog-validation/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "catalog_sha256": {
                "pattern": "^[0-9a-f]{64}$",
                "type": "string"
              },
              "format": {
                "const": "stove0-recipe-catalog-validation/v1"
              },
              "operation_count": {
                "minimum": 0,
                "type": "integer"
              },
              "recipe_count": {
                "minimum": 0,
                "type": "integer"
              },
              "recipes": {
                "items": {
                  "type": "object"
                },
                "type": "array"
              }
            },
            "required": [
              "format",
              "catalog_sha256",
              "operation_count",
              "recipe_count",
              "recipes"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/recipe/commands/validate/terminating_controls`

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
