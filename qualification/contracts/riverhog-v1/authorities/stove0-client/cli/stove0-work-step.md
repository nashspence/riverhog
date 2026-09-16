# stove0 work step

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-work-step:f02fcc0d00 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-bca0b1bae3"></a>Parser name: `step`
- <a id="s-1a43ee43a3"></a>Extra arguments at this parser: rejected.
- <a id="s-2a54840be1"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-283c618406"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-d3ccf333f4"></a>`work_id`<br>`work_id` | required positional; 1 value | text | not recorded |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-a1e3498f59"></a>`help` | <a id="s-06ba3ea128"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-534e6c6dba"></a>`0` | <a id="s-2cd53945ce"></a>`"noncontractual-framework-help"` | <a id="s-900b6f1ffd"></a>`"empty"` |

### Result and failure contract

- <a id="s-aebb92ccfd"></a>Result identity: `stove0-cli-result/work/step/v1`
- <a id="s-deaf41928e"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-acd771d760"></a>Structured output: `optional-json`
- <a id="s-babe5a626a"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-950b0f98ea"></a>`completed` | <a id="s-ddfb852747"></a>`{"kind":"command-completed"}` | <a id="s-90f5a6747c"></a>`0` | <a id="s-3a55413d01"></a>human: `noncontractual-presentation-of-command-result`; json: [HTTP step_work response 200](../../stove0/http-operations/post-v1-work-work-id-step.md#s-feaab7b4c6) | <a id="s-f364ede102"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-e9f51e67d3"></a>`usage` | <a id="s-f4c931acf5"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-9ef611ab8d"></a>`2` | <a id="s-d5040b0208"></a>all: `empty` | <a id="s-465ea0b327"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-3af47fb6b8"></a>`operational` | <a id="s-9246297c52"></a>`{"kind":"application-error"}` | <a id="s-90f1e1543f"></a>`1` | <a id="s-2c7b101c1b"></a>all: `empty` | <a id="s-0436b48149"></a>all: `noncontractual-diagnostic` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter work_id](#s-d3ccf333f4) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [POST /v1/work/{work_id}/step](../../stove0/http-operations/post-v1-work-work-id-step.md)
- [stove0_api_client.Stove0ApiClient.step_work](../../stove0-api-client/python/stove0-api-client-stove0apiclient-step-work.md)

## Governing policies

- <a id="pa-6a175a8310"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-f27113bd7b"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/stove0/application/client/src/stove0_cli/main.py::step_work](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py#L349)

### Machine authority

- `/external_contract/cli/stove0/commands/work/commands/step/allow_extra_args`
- `/external_contract/cli/stove0/commands/work/commands/step/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/work/commands/step/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/work/commands/step/name`
- `/external_contract/cli/stove0/commands/work/commands/step/parameters`
- `/external_contract/cli/stove0/commands/work/commands/step/result_contract`
- `/external_contract/cli/stove0/commands/work/commands/step/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/work/commands/step/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/work/commands/step/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/work/commands/step/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/work/commands/step/name`

<!-- exact-contract-value: 0d9f50d8178cb7c5b044c4dce43f1a35c44697ac03e70407e2dda2324fa92f56 -->

```json
"step"
```

### `/external_contract/cli/stove0/commands/work/commands/step/parameters`

<!-- exact-contract-value: 817c2e603d886c184e6cc1469f89564372168f34c065c797f0d455ecaece1909 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "work_id",
    "nargs": 1,
    "options": [
      "work_id"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  }
]
```

### `/external_contract/cli/stove0/commands/work/commands/step/result_contract`

<!-- exact-contract-value: 788eaafc9f99d0a74e7b925431aed31ea35d76d7623c28ca6f9a246f458342f1 -->

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
  "identity": "stove0-cli-result/work/step/v1",
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
          "operation_id": "step_work",
          "path": "/v1/work/{work_id}/step",
          "schema": {
            "$ref": "#/components/schemas/WorkView"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/work/commands/step/terminating_controls`

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
