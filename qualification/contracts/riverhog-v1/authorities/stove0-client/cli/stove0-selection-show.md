# stove0 selection show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-selection-show:58bde65483 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-631ffef902"></a>Parser name: `show`
- <a id="s-84961c98a7"></a>Extra arguments at this parser: rejected.
- <a id="s-c2f319c5ba"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-ce620badda"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-3a8e06a180"></a>`selection_sha256`<br>`selection_sha256` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-5f3ed31399"></a>`continuation`<br>`--continuation` | optional option; 1 value | text | not recorded<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-c17e8b12f7"></a>`help` | <a id="s-59885f30e5"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-cf7e66534e"></a>`0` | <a id="s-1ef2df6556"></a>`"noncontractual-framework-help"` | <a id="s-a539cdc0b0"></a>`"empty"` |

### Result and failure contract

- <a id="s-d210b84233"></a>Result identity: `stove0-cli-result/selection/show/v1`
- <a id="s-0b6284761d"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-a27d7b6dc8"></a>Structured output: `optional-json`
- <a id="s-ace4c7429c"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-c6c631bec6"></a>`completed` | <a id="s-ea28642db4"></a>`{"kind":"command-completed"}` | <a id="s-7823741763"></a>`0` | <a id="s-efde2ec553"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP get_artifact_selection response 200](../../stove0/http-operations/get-v1-artifact-selections-selection-sha256.md#s-3453778107) | <a id="s-d1d06225ac"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-fd43bf3579"></a>`usage` | <a id="s-17f7f65a97"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-c77811de56"></a>`2` | <a id="s-74e235e593"></a>all: `"empty"` | <a id="s-00f14ac540"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-2a69054a57"></a>`operational` | <a id="s-945853de8e"></a>`{"kind":"application-error"}` | <a id="s-6daeab7dea"></a>`1` | <a id="s-70784d34b1"></a>all: `"empty"` | <a id="s-f4d855a9a3"></a>all: `"noncontractual-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --continuation](#s-5f3ed31399) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter selection_sha256](#s-3a8e06a180) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/artifact-selections/{selection_sha256}](../../stove0/http-operations/get-v1-artifact-selections-selection-sha256.md)
- [stove0_api_client.Stove0ApiClient.get_artifact_selection](../../stove0-api-client/python/stove0-api-client-stove0apiclient-get-artifact-selection.md)

## Governing policies

- <a id="pa-63cdade1dd"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-3e7e3c6435"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/stove0/application/client/src/stove0_cli/main.py::get_artifact_selection](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py#L332)

### Machine authority

- `/external_contract/cli/stove0/commands/selection/commands/show/allow_extra_args`
- `/external_contract/cli/stove0/commands/selection/commands/show/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/selection/commands/show/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/selection/commands/show/name`
- `/external_contract/cli/stove0/commands/selection/commands/show/parameters`
- `/external_contract/cli/stove0/commands/selection/commands/show/result_contract`
- `/external_contract/cli/stove0/commands/selection/commands/show/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/selection/commands/show/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/selection/commands/show/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/selection/commands/show/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/selection/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/stove0/commands/selection/commands/show/parameters`

<!-- exact-contract-value: 0b10d00386773bfc4b66ab848fefb0e9ce0dfa31cd1793be7c11acef3238bc00 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "selection_sha256",
    "nargs": 1,
    "options": [
      "selection_sha256"
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
    "name": "continuation",
    "nargs": 1,
    "options": [
      "--continuation"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  }
]
```

### `/external_contract/cli/stove0/commands/selection/commands/show/result_contract`

<!-- exact-contract-value: e85505ebd50b88d6695e65fac1bc896e9bb4a29e8b416d81574fc454fb65f1a9 -->

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
  "identity": "stove0-cli-result/selection/show/v1",
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
          "method": "GET",
          "operation_id": "get_artifact_selection",
          "path": "/v1/artifact-selections/{selection_sha256}",
          "schema": {
            "$ref": "#/components/schemas/ArtifactSelectionPage"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/selection/commands/show/terminating_controls`

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
