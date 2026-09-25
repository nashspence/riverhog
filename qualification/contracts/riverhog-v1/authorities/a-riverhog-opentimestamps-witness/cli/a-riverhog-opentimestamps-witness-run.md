# a-riverhog-opentimestamps-witness run

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-opentimestamps-witness:a-riverhog-opentimestamps-witness-run:ed25555e4f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-opentimestamps-witness](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-eeb7e42a50"></a>Parser name: `run`
- <a id="s-e7ca18234e"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-678b04db1c"></a>`calendar`<br>`--calendar` | required option; 1 value; collects repeats; no declared occurrence maximum | not recorded | not recorded |
| <a id="s-69a36bba3f"></a>`poll_seconds`<br>`--poll-seconds` | optional option; 1 value | int | `60` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-348b919d5b"></a>`help` | <a id="s-bd39b7c1d4"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-f147a626c6"></a>`0` | <a id="s-d36b94204f"></a>`"noncontractual-framework-help"` | <a id="s-813399f17e"></a>`"empty"` |

### Result and failure contract

- <a id="s-f2776a64cc"></a>Result identity: `a-riverhog-opentimestamps-witness-cli-result/run/v1`
- <a id="s-f9da538e64"></a>Profile: `a-riverhog-opentimestamps-witness-cli-runtime/v1`
- <a id="s-ed47f5e8ef"></a>Structured output: `none`
- <a id="s-0e676b42ed"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-1af09e6464"></a>`runtime-returned` | <a id="s-575cd62d84"></a>`{"kind":"service-runtime-returned"}` | <a id="s-68715c345d"></a>`0` | <a id="s-ffc7a9f630"></a>all: `"no-command-result"` | <a id="s-453dbce7ac"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-1217ddf421"></a>`usage` | <a id="s-fbf610fe04"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-2f78e57132"></a>`2` | <a id="s-79602fca7b"></a>all: `"empty"` | <a id="s-e608a030ef"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-375bb55180"></a>`application-error` | <a id="s-e1126f1b93"></a>`{"kind":"application-error"}` | <a id="s-3f8449a743"></a>`1` | <a id="s-92e3df6acb"></a>all: `"empty"` | <a id="s-e5711da75c"></a>all: `"noncontractual-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"a-riverhog-opentimestamps-witness"}; maximum=null; reason="no-declared-semantic-maximum"; source_constraint={"field":"kind"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --calendar](#s-678b04db1c) | `cardinality · occurrences · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --calendar](#s-678b04db1c) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --poll-seconds](#s-69a36bba3f) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-11521d6066"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-83347c8483"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-d5224e3ff3"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-opentimestamps-witness](../../../evidence/sources/authorities.md#src-26e499502f) — [some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a\_riverhog\_opentimestamps\_witness/cli.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a_riverhog_opentimestamps_witness/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/run/allow_abbrev`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/run/name`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/run/parameters`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/run/result_contract`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/run/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/run/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/run/name`

<!-- exact-contract-value: 5e87f618bd8837e87070ae7f83753c6a23ff095f43de6ededcd38ae535031c29 -->

```json
"run"
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/run/parameters`

<!-- exact-contract-value: a52c3d702124646f760564b742c13e5f30d2ac089c0b18d95ff23b0da20488e3 -->

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
    "default": 60,
    "dest": "poll_seconds",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--poll-seconds"
    ],
    "required": false,
    "type": "int"
  }
]
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/run/result_contract`

<!-- exact-contract-value: 726acf0b6f49575342922c506af559ebea14ef54ac3d9c8f4cf0dab390081b4a -->

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
  "identity": "a-riverhog-opentimestamps-witness-cli-result/run/v1",
  "profile_id": "a-riverhog-opentimestamps-witness-cli-runtime/v1",
  "structured_output": "none",
  "success": [
    {
      "exit_status": 0,
      "id": "runtime-returned",
      "selected_by": {
        "kind": "service-runtime-returned"
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "all": "no-command-result"
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/run/terminating_controls`

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
