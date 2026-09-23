# a-stove0-nvenc-av1-opus-target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-stove0-nvenc-av1-opus-target:a-stove0-nvenc-av1-opus-target:47354b466e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-24aa31b764"></a>Parser name: `a-stove0-nvenc-av1-opus-target`
- <a id="s-fa65c5bd25"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-fe8bf5ada3"></a>`host`<br>`--host` | optional option; 1 value | not recorded | `"127.0.0.1"` |
| <a id="s-fa74cbbc35"></a>`port`<br>`--port` | optional option; 1 value | int | `8080` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-bbfa7f0965"></a>`help` | <a id="s-1a5e9ead5f"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-440c11a432"></a>`0` | <a id="s-bbb6cd6ba9"></a>`"noncontractual-framework-help"` | <a id="s-c68134f8e9"></a>`"empty"` |
| <a id="s-d37bbe38a8"></a>`version` | <a id="s-3358c21327"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-a18091392c"></a>`0` | <a id="s-316a0c6749"></a>`{"distribution":"a-stove0-nvenc-av1-opus-target","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-f532dbd68d"></a>`"empty"` |

### Result and failure contract

- <a id="s-f0d84411a5"></a>Result identity: `a-stove0-nvenc-av1-opus-target-cli-result/root/v1`
- <a id="s-d9682200d0"></a>Profile: `a-stove0-nvenc-av1-opus-target-cli-runtime/v1`
- <a id="s-bb69900c70"></a>Structured output: `none`
- <a id="s-4fd43092ca"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-761e9fe033"></a>`stopped` | <a id="s-a10d6b3a4b"></a>`{"kind":"service-runtime-returned"}` | <a id="s-d2bddc8456"></a>`0` | <a id="s-70d19f5647"></a>all: `"no-command-result"` | <a id="s-1e178a0265"></a>all: `"noncontractual-runtime-log"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-6197716f9e"></a>`usage` | <a id="s-a248aa6f7f"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-5204b6583c"></a>`2` | <a id="s-e12508c2c4"></a>all: `"empty"` | <a id="s-e8396704f7"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --host](#s-fe8bf5ada3) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --port](#s-fa74cbbc35) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-24883bb517"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-816968cc64"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-stove0-nvenc-av1-opus-target](../../../evidence/sources/authorities.md#src-95e636daf5) — [some-implementations/stove0/targets/nvenc-av1-opus/target/src/a\_stove0\_nvenc\_av1\_opus\_target/app.py::&lt;module&gt;](../../../../../../some-implementations/stove0/targets/nvenc-av1-opus/target/src/a_stove0_nvenc_av1_opus_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-stove0-nvenc-av1-opus-target/allow_abbrev`
- `/external_contract/cli/a-stove0-nvenc-av1-opus-target/name`
- `/external_contract/cli/a-stove0-nvenc-av1-opus-target/parameters`
- `/external_contract/cli/a-stove0-nvenc-av1-opus-target/result_contract`
- `/external_contract/cli/a-stove0-nvenc-av1-opus-target/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-stove0-nvenc-av1-opus-target/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-stove0-nvenc-av1-opus-target/name`

<!-- exact-contract-value: d952cd35067e812279329bf66eff5ea1029e4cb5c12d2d88d0fed4c017dd5e72 -->

```json
"a-stove0-nvenc-av1-opus-target"
```

### `/external_contract/cli/a-stove0-nvenc-av1-opus-target/parameters`

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

### `/external_contract/cli/a-stove0-nvenc-av1-opus-target/result_contract`

<!-- exact-contract-value: 7585bad39ba9b24a68cf1db94ea36344e08a42240857ab3f278a8c5deb99cf1b -->

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
  "identity": "a-stove0-nvenc-av1-opus-target-cli-result/root/v1",
  "profile_id": "a-stove0-nvenc-av1-opus-target-cli-runtime/v1",
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

### `/external_contract/cli/a-stove0-nvenc-av1-opus-target/terminating_controls`

<!-- exact-contract-value: b120befd7c4fc9be051ae30de47d59a4f46e853288a73157beee4dbdfb64d995 -->

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
      "distribution": "a-stove0-nvenc-av1-opus-target",
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
