# a-riverhog-opentimestamps-witness reschedule

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-opentimestamps-witness:a-riverhog-opentimestamps-witness-reschedule:ac7cd6aab4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-opentimestamps-witness](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-1c6b41a145"></a>Parser name: `reschedule`
- <a id="s-4700cff201"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-dd6835c10f"></a>`calendar`<br>`--calendar` | required option; 1 value; collects repeats; no declared occurrence maximum | not recorded | not recorded |
| <a id="s-a439f62c01"></a>`digest` | required positional; 1 value | not recorded | not recorded |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-814a47d231"></a>`help` | <a id="s-8888b7e24e"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-3e75206657"></a>`0` | <a id="s-6f90208503"></a>`"noncontractual-framework-help"` | <a id="s-6ff6665f22"></a>`"empty"` |

### Result and failure contract

- <a id="s-00cb16f87e"></a>Result identity: `a-riverhog-opentimestamps-witness-cli-result/reschedule/v1`
- <a id="s-de670ddb2f"></a>Profile: `a-riverhog-opentimestamps-witness-cli-json/v1`
- <a id="s-f9b2b946a3"></a>Structured output: `always-json`
- <a id="s-262efcf5c6"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-e634ff6041"></a>`completed` | <a id="s-4bace0a6b5"></a>`{"kind":"command-completed"}` | <a id="s-ce2a2704e1"></a>`0` | <a id="s-373f13c41f"></a>json: [a-riverhog-opentimestamps-witness-cli-reschedule/v1](#s-df8ab8bf76) | <a id="s-44559db1ef"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-ee03e07a20"></a>`usage` | <a id="s-912d0a9feb"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-4ef367bfba"></a>`2` | <a id="s-791cf3c8ab"></a>all: `"empty"` | <a id="s-03e796393c"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-434f15ccde"></a>`application-error` | <a id="s-dde711450e"></a>`{"kind":"application-error"}` | <a id="s-68a32ef759"></a>`1` | <a id="s-b6c21bef87"></a>all: `"empty"` | <a id="s-c25000798a"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-df8ab8bf76"></a>`a-riverhog-opentimestamps-witness-cli-reschedule/v1`

Applies to: completed · stdout (json).

<a id="s-5c7b91ce21"></a>

- <a id="s-93eb6334ba"></a>`type`: `"object"`
- <a id="s-16fd021891"></a>`additionalProperties`: `false`
- <a id="s-f1296c22e2"></a>`required`: `["rescheduled"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9d3aca22b3"></a>`rescheduled` | yes | type="boolean" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"a-riverhog-opentimestamps-witness"}; maximum=null; reason="no-declared-semantic-maximum"; source_constraint={"field":"kind"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --calendar](#s-dd6835c10f) | `cardinality · occurrences · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --calendar](#s-dd6835c10f) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter 1](#s-a439f62c01) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-1022029e05"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-029c3dc59b"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-625b9cdf9b"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-opentimestamps-witness](../../../evidence/sources/authorities.md#src-26e499502f) — [some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a\_riverhog\_opentimestamps\_witness/cli.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a_riverhog_opentimestamps_witness/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/reschedule/allow_abbrev`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/reschedule/name`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/reschedule/parameters`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/reschedule/result_contract`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/reschedule/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/reschedule/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/reschedule/name`

<!-- exact-contract-value: 004330b96b149fd3839e2579159088579210a0ec7c0845698b6e55af813a5785 -->

```json
"reschedule"
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/reschedule/parameters`

<!-- exact-contract-value: e8a4dde3311eb35cca047b9c857ef609001041342785ac683f293663b1f66821 -->

```json
[
  {
    "dest": "calendar",
    "kind": "_AppendAction",
    "nargs": null,
    "options": [
      "--calendar"
    ],
    "required": true
  },
  {
    "dest": "digest",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [],
    "required": true
  }
]
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/reschedule/result_contract`

<!-- exact-contract-value: 8edc696bee5f076e3e79a45048fdc2ce76d3c9d63423ffeb5c91d2d3461ebbf4 -->

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
      "id": "application-error",
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
  "human_json_relationship": "not-applicable",
  "identity": "a-riverhog-opentimestamps-witness-cli-result/reschedule/v1",
  "profile_id": "a-riverhog-opentimestamps-witness-cli-json/v1",
  "structured_output": "always-json",
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
        "json": {
          "identity": "a-riverhog-opentimestamps-witness-cli-reschedule/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "rescheduled": {
                "type": "boolean"
              }
            },
            "required": [
              "rescheduled"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/reschedule/terminating_controls`

<!-- exact-contract-value: 46c96c22d2ed8a51da57bba3ac0f2269bb35f98c5fd6dc3e3ba67e60f772ad72 -->

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
        "-h",
        "--help"
      ]
    }
  }
]
```

</details>
