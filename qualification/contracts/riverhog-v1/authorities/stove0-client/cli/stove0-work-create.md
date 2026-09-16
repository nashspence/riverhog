# stove0 work create

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-work-create:706f8c3f36 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-168e30cf75"></a>Parser name: `create`
- <a id="s-e1abdffe7c"></a>Extra arguments at this parser: rejected.
- <a id="s-fcb4cc9ac8"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-286b25c419"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-688412187a"></a>`recipe_id`<br>`recipe_id` | required positional; 1 value | text | not recorded |
| <a id="s-2e82af2b50"></a>`inputs`<br>`inputs` | required positional; 1+ values; no parser maximum | text | not recorded |
| <a id="s-ad519fa0aa"></a>`preview_sha256`<br>`--preview-sha256` | required option; 1 value | text | not recorded |
| <a id="s-cade0041ae"></a>`revision`<br>`--revision` | optional option; 1 value | integer | not recorded |
| <a id="s-ee4bb44d8c"></a>`intent`<br>`--intent` | optional option; 1 value | file; existence required; regular files allowed; directories rejected; access checks on existing paths: read; resolve absolute path and symlinks: no; dash uses normal path checks | not recorded |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-f8cc125f51"></a>`help` | <a id="s-fffa832d42"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-693303befa"></a>`0` | <a id="s-a9d8988abe"></a>`"noncontractual-framework-help"` | <a id="s-1ad5ff677f"></a>`"empty"` |

### Result and failure contract

- <a id="s-9f5724ea86"></a>Result identity: `stove0-cli-result/work/create/v1`
- <a id="s-c61ce4c12c"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-c472a0d09b"></a>Structured output: `optional-json`
- <a id="s-4bd3946a13"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-c4ab377554"></a>`completed` | <a id="s-985f74765b"></a>`{"kind":"command-completed"}` | <a id="s-3798959ba0"></a>`0` | <a id="s-6569bd933d"></a>human: `noncontractual-presentation-of-command-result`; json: [HTTP create_work response 201](../../stove0/http-operations/post-v1-work.md#s-bc8a3b692a) | <a id="s-20ece9866b"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-54c1e7f258"></a>`usage` | <a id="s-6cf2fe0e6e"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-a1b3264f27"></a>`2` | <a id="s-ec3b5d8f4d"></a>all: `empty` | <a id="s-1c2a25b1f7"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-ae9521b5ac"></a>`operational` | <a id="s-b3400a929b"></a>`{"kind":"application-error"}` | <a id="s-cb4de05fbd"></a>`1` | <a id="s-607a6bc870"></a>all: `empty` | <a id="s-edaf1b3981"></a>all: `noncontractual-diagnostic` |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; minimum=1; reason="no-declared-semantic-maximum"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter inputs](#s-2e82af2b50) | `cardinality · values-per-occurrence · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --intent](#s-ee4bb44d8c) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --preview-sha256](#s-ad519fa0aa) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter recipe_id](#s-688412187a) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --revision](#s-cade0041ae) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [POST /v1/work](../../stove0/http-operations/post-v1-work.md)
- [stove0_api_client.Stove0ApiClient.create_work](../../stove0-api-client/python/stove0-api-client-stove0apiclient-create-work.md)

## Governing policies

- <a id="pa-913bb74f8b"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-fe11e087d1"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-518c856370"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/stove0/application/client/src/stove0_cli/main.py::create_work](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py#L295)

### Machine authority

- `/external_contract/cli/stove0/commands/work/commands/create/allow_extra_args`
- `/external_contract/cli/stove0/commands/work/commands/create/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/work/commands/create/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/work/commands/create/name`
- `/external_contract/cli/stove0/commands/work/commands/create/parameters`
- `/external_contract/cli/stove0/commands/work/commands/create/result_contract`
- `/external_contract/cli/stove0/commands/work/commands/create/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/work/commands/create/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/work/commands/create/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/work/commands/create/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/work/commands/create/name`

<!-- exact-contract-value: 5498a731a187f424a5800943afcba027f3a6cd684e38fe6e40c02bee1753152d -->

```json
"create"
```

### `/external_contract/cli/stove0/commands/work/commands/create/parameters`

<!-- exact-contract-value: 74e38f06d84a13ee839b703c7aedf8ecd8fc6c7d384c0321c67e5e06ff52aa64 -->

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
    "name": "preview_sha256",
    "nargs": 1,
    "options": [
      "--preview-sha256"
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

### `/external_contract/cli/stove0/commands/work/commands/create/result_contract`

<!-- exact-contract-value: 96ebf33c5fa65cd238ca6751a4261bd61268e2ad442604f1e7aa27e81b8102c9 -->

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
  "identity": "stove0-cli-result/work/create/v1",
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
          "operation_id": "create_work",
          "path": "/v1/work",
          "schema": {
            "$ref": "#/components/schemas/WorkView"
          },
          "status": "201"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/work/commands/create/terminating_controls`

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
