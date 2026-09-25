# a-riverhog-opentimestamps-witness mature

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-opentimestamps-witness:a-riverhog-opentimestamps-witness-mature:b3833708f9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-opentimestamps-witness](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-2127c4de53"></a>Parser name: `mature`
- <a id="s-32655e4ede"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-c73a32912f"></a>`calendar`<br>`--calendar` | required option; 1 value; collects repeats; no declared occurrence maximum | not recorded | not recorded |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-5564e8f6f4"></a>`help` | <a id="s-4e034c063a"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-dbb1dee20b"></a>`0` | <a id="s-d97331e1ca"></a>`"noncontractual-framework-help"` | <a id="s-5f34c3b517"></a>`"empty"` |

### Result and failure contract

- <a id="s-98ca7c18c0"></a>Result identity: `a-riverhog-opentimestamps-witness-cli-result/mature/v1`
- <a id="s-9ff5f96df4"></a>Profile: `a-riverhog-opentimestamps-witness-cli-json/v1`
- <a id="s-7d89e75736"></a>Structured output: `always-json`
- <a id="s-2b3985cff6"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-b4430565f3"></a>`completed` | <a id="s-6f5509c524"></a>`{"kind":"command-completed"}` | <a id="s-c7efd90605"></a>`0` | <a id="s-0d22426668"></a>json: [a-riverhog-opentimestamps-witness-cli-mature/v1](#s-c7e50347f7) | <a id="s-c6602971a8"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-6fddf0f0d3"></a>`usage` | <a id="s-590f967b65"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-ef7c6b8f8e"></a>`2` | <a id="s-5a0695ba14"></a>all: `"empty"` | <a id="s-6d0e0d6fd8"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-8e0b66ee9f"></a>`application-error` | <a id="s-e83f5d4200"></a>`{"kind":"application-error"}` | <a id="s-363b579784"></a>`1` | <a id="s-42a401e09d"></a>all: `"empty"` | <a id="s-12eedb1a1e"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-c7e50347f7"></a>`a-riverhog-opentimestamps-witness-cli-mature/v1`

Applies to: completed · stdout (json).

<a id="s-6452ad261f"></a>

- <a id="s-20b36a50d5"></a>`type`: `"object"`
- <a id="s-8a676cf9f1"></a>`additionalProperties`: `false`
- <a id="s-16cf68631b"></a>`required`: `["matured_statement"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-50fb7325dc"></a>`matured_statement` | yes | type=["string","null"] |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"a-riverhog-opentimestamps-witness"}; maximum=null; reason="no-declared-semantic-maximum"; source_constraint={"field":"kind"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --calendar](#s-c73a32912f) | `cardinality · occurrences · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --calendar](#s-c73a32912f) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-396b021490"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-05f2df2a2c"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-018d1c4bea"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-opentimestamps-witness](../../../evidence/sources/authorities.md#src-26e499502f) — [some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a\_riverhog\_opentimestamps\_witness/cli.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a_riverhog_opentimestamps_witness/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/mature/allow_abbrev`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/mature/name`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/mature/parameters`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/mature/result_contract`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/mature/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/mature/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/mature/name`

<!-- exact-contract-value: 2f3435e6269dfcc6337d36c427304c55d6ad7c7df7a70fde7b7ce3c8bc49977a -->

```json
"mature"
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/mature/parameters`

<!-- exact-contract-value: 7f5406d53072b5be31fa3b34d53b9714fcf1839c2f13bccfbaf52d0853e6cc78 -->

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
  }
]
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/mature/result_contract`

<!-- exact-contract-value: e6a081f58d87c3782e5a5c0d514309b997ee605cecf068e1c5200b7acf9ef615 -->

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
  "identity": "a-riverhog-opentimestamps-witness-cli-result/mature/v1",
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
          "identity": "a-riverhog-opentimestamps-witness-cli-mature/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "matured_statement": {
                "type": [
                  "string",
                  "null"
                ]
              }
            },
            "required": [
              "matured_statement"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/mature/terminating_controls`

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
