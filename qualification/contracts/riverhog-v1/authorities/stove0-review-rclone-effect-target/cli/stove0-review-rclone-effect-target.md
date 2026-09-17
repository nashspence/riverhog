# stove0-review-rclone-effect-target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target:81ce3625b9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-12ae1ac5ce"></a>Parser name: `stove0-review-rclone-effect-target`
- <a id="s-67e82ac688"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-cfad382630"></a>`host`<br>`--host` | optional option; 1 value | not recorded | `"127.0.0.1"` |
| <a id="s-f41aaf58e4"></a>`port`<br>`--port` | optional option; 1 value | int | `8080` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-49559a68a3"></a>`help` | <a id="s-f143880562"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-872796af0e"></a>`0` | <a id="s-d29d4c77fb"></a>`"noncontractual-framework-help"` | <a id="s-252e7d4bc8"></a>`"empty"` |
| <a id="s-0111e6beae"></a>`version` | <a id="s-888f93a1d7"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-2883aadfb6"></a>`0` | <a id="s-5861643e05"></a>`{"distribution":"stove0-review-rclone-effect-target","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-6be3685ebd"></a>`"empty"` |

### Result and failure contract

- <a id="s-ee7f03433f"></a>Result identity: `stove0-review-rclone-effect-target-cli-result/root/v1`
- <a id="s-ed68b98e31"></a>Profile: `stove0-review-rclone-effect-target-cli-runtime/v1`
- <a id="s-3c42a0fd64"></a>Structured output: `none`
- <a id="s-71662cd9ef"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-ba2d50a241"></a>`stopped` | <a id="s-e097b3d0ec"></a>`{"kind":"service-runtime-returned"}` | <a id="s-834f7598ae"></a>`0` | <a id="s-8a772e8dd4"></a>all: `"no-command-result"` | <a id="s-fc17c82bd7"></a>all: `"noncontractual-runtime-log"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-a1a4ba2e41"></a>`usage` | <a id="s-e3bf07290f"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-f2310386fd"></a>`2` | <a id="s-873be6ae39"></a>all: `"empty"` | <a id="s-9d35fcb8cb"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --host](#s-cfad382630) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --port](#s-f41aaf58e4) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-cd251b9f5a"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-8639d006a7"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-review-rclone-effect-target](../../../evidence/sources/authorities.md#src-f14fced74d) — [reference/stove0/targets/review/rclone-effect-target/src/stove0\_review\_rclone\_effect\_target/app.py::&lt;module&gt;](../../../../../../reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/stove0-review-rclone-effect-target/allow_abbrev`
- `/external_contract/cli/stove0-review-rclone-effect-target/name`
- `/external_contract/cli/stove0-review-rclone-effect-target/parameters`
- `/external_contract/cli/stove0-review-rclone-effect-target/result_contract`
- `/external_contract/cli/stove0-review-rclone-effect-target/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-review-rclone-effect-target/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0-review-rclone-effect-target/name`

<!-- exact-contract-value: cd476f92402e15739b1cff2efc426dd10a99be6ba2e01c24f5e4d5c9b4ccbe60 -->

```json
"stove0-review-rclone-effect-target"
```

### `/external_contract/cli/stove0-review-rclone-effect-target/parameters`

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

### `/external_contract/cli/stove0-review-rclone-effect-target/result_contract`

<!-- exact-contract-value: cfdffd1d703119c1e740c47ae2144318790f2679b023c68cfd35f291ced900cf -->

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
  "identity": "stove0-review-rclone-effect-target-cli-result/root/v1",
  "profile_id": "stove0-review-rclone-effect-target-cli-runtime/v1",
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

### `/external_contract/cli/stove0-review-rclone-effect-target/terminating_controls`

<!-- exact-contract-value: 1c63b6986006480d49f3731d40f189a69c486b4eff06207abdb8174a464b37fe -->

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
      "distribution": "stove0-review-rclone-effect-target",
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
