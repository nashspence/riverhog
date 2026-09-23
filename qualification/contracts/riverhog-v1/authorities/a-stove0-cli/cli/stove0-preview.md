# stove0 preview

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-stove0-cli:stove0-preview:abfc39632b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-df3533fcff"></a>Parser name: `preview`
- <a id="s-cc57e33030"></a>Extra arguments at this parser: rejected.
- <a id="s-8d4560df93"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-135760281e"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-90627304a6"></a>`recipe_id`<br>`recipe_id` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-f6a4b5a531"></a>`inputs`<br>`inputs` | required positional; 1+ values; no parser maximum | text | not recorded<br>Env: `null` |
| <a id="s-4f9f8ea3f9"></a>`revision`<br>`--revision` | optional option; 1 value | integer | not recorded<br>Env: `null` |
| <a id="s-4ed179ad38"></a>`intent`<br>`--intent` | optional option; 1 value | file; existence required; regular files allowed; directories rejected; access checks on existing paths: read; resolve absolute path and symlinks: no; dash uses normal path checks | not recorded<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-e1261405ed"></a>`help` | <a id="s-d2883679de"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-e5895969c6"></a>`0` | <a id="s-ebbc22b918"></a>`"noncontractual-framework-help"` | <a id="s-69c2f1ab83"></a>`"empty"` |

### Result and failure contract

- <a id="s-f0d6d9f631"></a>Result identity: `stove0-cli-result/preview/v1`
- <a id="s-d94100912f"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-90e981514f"></a>Structured output: `optional-json`
- <a id="s-3ff143de55"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-2f61bb447e"></a>`completed` | <a id="s-e1e381c17e"></a>`{"kind":"command-completed"}` | <a id="s-dc75be26dd"></a>`0` | <a id="s-90f8319fa1"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP preview_workflow response 200](../../stove0/http-operations/post-v1-workflow-previews.md#s-9a578ffce8) | <a id="s-2a3db21a11"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-eecaf28643"></a>`usage` | <a id="s-e980ace003"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-0f3aff7714"></a>`2` | <a id="s-985ca4bcd1"></a>all: `"empty"` | <a id="s-59db1ec1ac"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-49a0fea184"></a>`operational` | <a id="s-b417ade6c5"></a>`{"kind":"application-error"}` | <a id="s-4cbe8d680b"></a>`1` | <a id="s-fc7a2b7d11"></a>all: `"empty"` | <a id="s-9a97297e4b"></a>all: `"noncontractual-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; minimum=1; reason="no-declared-semantic-maximum"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter inputs](#s-f6a4b5a531) | `cardinality · values-per-occurrence · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --intent](#s-4ed179ad38) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter recipe_id](#s-90627304a6) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --revision](#s-4f9f8ea3f9) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [POST /v1/workflow-previews](../../stove0/http-operations/post-v1-workflow-previews.md)
- [stove0_api_client.Stove0ApiClient.preview_workflow](../../stove0-api-client/python/stove0-api-client-stove0apiclient-preview-workflow.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-74111163cf"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-754e1c9400"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-96445687ae"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources/authorities.md#src-6203ae7d88) — [some-implementations/stove0/application/client/src/a\_stove0\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/stove0/application/client/src/a_stove0_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/stove0/application/client/src/a\_stove0\_cli/main.py::preview](../../../../../../some-implementations/stove0/application/client/src/a_stove0_cli/main.py#L370)

### Machine authority

- `/external_contract/cli/stove0/commands/preview/allow_extra_args`
- `/external_contract/cli/stove0/commands/preview/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/preview/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/preview/name`
- `/external_contract/cli/stove0/commands/preview/parameters`
- `/external_contract/cli/stove0/commands/preview/result_contract`
- `/external_contract/cli/stove0/commands/preview/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/preview/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/preview/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/preview/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/preview/name`

<!-- exact-contract-value: 99e505dc299c68bc16363d9d8ee5b76599ce41e2154ddb0dc8e2a00c57dec7a5 -->

```json
"preview"
```

### `/external_contract/cli/stove0/commands/preview/parameters`

<!-- exact-contract-value: 6e8fb5690ec3391c482598c40125324a9f58ec56360a3c2245bc0a41dd061ca1 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "recipe_id",
    "nargs": 1,
    "options": [
      "recipe_id"
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
    "name": "inputs",
    "nargs": -1,
    "options": [
      "inputs"
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
      "class": "typer._click.types.IntParamType",
      "name": "integer"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "intent",
    "nargs": 1,
    "options": [
      "--intent"
    ],
    "required": false,
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

### `/external_contract/cli/stove0/commands/preview/result_contract`

<!-- exact-contract-value: 24a9b59c5e38fec052e023784c05a682632be5a669e6b2cb944908203686a6e2 -->

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
  "identity": "stove0-cli-result/preview/v1",
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
          "application": "stove0",
          "kind": "http-operation-response",
          "method": "POST",
          "operation_id": "preview_workflow",
          "path": "/v1/workflow-previews",
          "schema": {
            "$ref": "#/components/schemas/WorkflowPreview"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/preview/terminating_controls`

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
