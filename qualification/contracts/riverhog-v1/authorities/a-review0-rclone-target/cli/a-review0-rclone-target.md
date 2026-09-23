# a-review0-rclone-target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-review0-rclone-target:a-review0-rclone-target:0b501b2554 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-rclone-target](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-721637a58f"></a>Parser name: `a-review0-rclone-target`
- <a id="s-6230bb5ff4"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-421b2c6e52"></a>`host`<br>`--host` | optional option; 1 value | not recorded | `"127.0.0.1"` |
| <a id="s-df475762a7"></a>`port`<br>`--port` | optional option; 1 value | int | `8080` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-a54ba491d5"></a>`help` | <a id="s-4098c8ef9d"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-d54fb13b60"></a>`0` | <a id="s-8d0b21f3a9"></a>`"noncontractual-framework-help"` | <a id="s-a86be1b385"></a>`"empty"` |
| <a id="s-054975da4c"></a>`version` | <a id="s-ba4f1007cb"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-4ef884292a"></a>`0` | <a id="s-84aaf98c32"></a>`{"distribution":"a-review0-rclone-target","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-c5af79f1c5"></a>`"empty"` |

### Result and failure contract

- <a id="s-8a35aecffb"></a>Result identity: `a-review0-rclone-target-cli-result/root/v1`
- <a id="s-53b9aa2b46"></a>Profile: `a-review0-rclone-target-cli-runtime/v1`
- <a id="s-5209cddfeb"></a>Structured output: `none`
- <a id="s-e4ab4276b5"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-02394f09f7"></a>`stopped` | <a id="s-9a4ce216cd"></a>`{"kind":"service-runtime-returned"}` | <a id="s-0e4eb3d7e5"></a>`0` | <a id="s-a1c9b4fee7"></a>all: `"no-command-result"` | <a id="s-3885c01d1f"></a>all: `"noncontractual-runtime-log"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-34eaaf8686"></a>`usage` | <a id="s-3c9593259a"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-693fb32121"></a>`2` | <a id="s-c27abd3387"></a>all: `"empty"` | <a id="s-8efa968b61"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --host](#s-421b2c6e52) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --port](#s-df475762a7) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-31e536853f"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-4a095d8f22"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-review0-rclone-target](../../../evidence/sources/authorities.md#src-84f287f2d6) — [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py::&lt;module&gt;](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-review0-rclone-target/allow_abbrev`
- `/external_contract/cli/a-review0-rclone-target/name`
- `/external_contract/cli/a-review0-rclone-target/parameters`
- `/external_contract/cli/a-review0-rclone-target/result_contract`
- `/external_contract/cli/a-review0-rclone-target/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-review0-rclone-target/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-review0-rclone-target/name`

<!-- exact-contract-value: 01a91f09145102df1733c8d310447f89668c905db07b223bcd91722e177baf4d -->

```json
"a-review0-rclone-target"
```

### `/external_contract/cli/a-review0-rclone-target/parameters`

<!-- exact-contract-value: ecc6f99dbe14513025a57c9b575b12b7e3c3add71a319df647c17cd2f15bcd28 -->

```json
[
  {
    "default": "127.0.0.1",
    "dest": "host",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--host"
    ],
    "required": false
  },
  {
    "default": 8080,
    "dest": "port",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--port"
    ],
    "required": false,
    "type": "int"
  }
]
```

### `/external_contract/cli/a-review0-rclone-target/result_contract`

<!-- exact-contract-value: cae49ac910d5da7b2140385595ad6abe75d44c03428ceca0b1c9456bfda9f905 -->

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
    }
  ],
  "human_json_relationship": "not-applicable",
  "identity": "a-review0-rclone-target-cli-result/root/v1",
  "profile_id": "a-review0-rclone-target-cli-runtime/v1",
  "structured_output": "none",
  "success": [
    {
      "exit_status": 0,
      "id": "stopped",
      "selected_by": {
        "kind": "service-runtime-returned"
      },
      "stderr": {
        "all": "noncontractual-runtime-log"
      },
      "stdout": {
        "all": "no-command-result"
      }
    }
  ]
}
```

### `/external_contract/cli/a-review0-rclone-target/terminating_controls`

<!-- exact-contract-value: da52f67e6268859a3a1436d19aeb7c9fc63800c0cf68517ad531709b4856e8dd -->

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
  },
  {
    "exit_status": 0,
    "id": "version",
    "stderr": "empty",
    "stdout": {
      "distribution": "a-review0-rclone-target",
      "kind": "installed-coordinated-release-version",
      "serialization": "noncontractual"
    },
    "trigger": {
      "kind": "option-present",
      "options": [
        "--version"
      ]
    }
  }
]
```

</details>
