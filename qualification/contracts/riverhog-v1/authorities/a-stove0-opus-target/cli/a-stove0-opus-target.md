# a-stove0-opus-target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-stove0-opus-target:a-stove0-opus-target:335e0f9008 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-opus-target](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-feae20842e"></a>Parser name: `a-stove0-opus-target`
- <a id="s-f4033cddb3"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-414c4de5d7"></a>`host`<br>`--host` | optional option; 1 value | not recorded | `"127.0.0.1"` |
| <a id="s-3e34a84d49"></a>`port`<br>`--port` | optional option; 1 value | int | `8080` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-e2af5f70bb"></a>`help` | <a id="s-f57c3552ff"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-9dd949d814"></a>`0` | <a id="s-740aaa918d"></a>`"noncontractual-framework-help"` | <a id="s-2d798cc455"></a>`"empty"` |
| <a id="s-6db1d7a443"></a>`version` | <a id="s-f3d1f9ae91"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-a7e58a1b5c"></a>`0` | <a id="s-9b64c4639e"></a>`{"distribution":"a-stove0-opus-target","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-0d3fc44e82"></a>`"empty"` |

### Result and failure contract

- <a id="s-47f9fe1afb"></a>Result identity: `a-stove0-opus-target-cli-result/root/v1`
- <a id="s-5ec40a2075"></a>Profile: `a-stove0-opus-target-cli-runtime/v1`
- <a id="s-85fae5ea01"></a>Structured output: `none`
- <a id="s-fe545e9249"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-f7435fea68"></a>`stopped` | <a id="s-d86095e545"></a>`{"kind":"service-runtime-returned"}` | <a id="s-1f427c720e"></a>`0` | <a id="s-9cced943c7"></a>all: `"no-command-result"` | <a id="s-ed223b4dc0"></a>all: `"noncontractual-runtime-log"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-a1da81c44a"></a>`usage` | <a id="s-159e9fa338"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-aa516c38f8"></a>`2` | <a id="s-098233a51b"></a>all: `"empty"` | <a id="s-8fac32e024"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --host](#s-414c4de5d7) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --port](#s-3e34a84d49) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-b169b62e49"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-28a0140caa"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-stove0-opus-target](../../../evidence/sources/authorities.md#src-b656cbd932) — [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/app.py::&lt;module&gt;](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-stove0-opus-target/allow_abbrev`
- `/external_contract/cli/a-stove0-opus-target/name`
- `/external_contract/cli/a-stove0-opus-target/parameters`
- `/external_contract/cli/a-stove0-opus-target/result_contract`
- `/external_contract/cli/a-stove0-opus-target/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-stove0-opus-target/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-stove0-opus-target/name`

<!-- exact-contract-value: 33a38370e24820524ba4225e441cc5af5e0e1d087654b6b70936d29876e51819 -->

```json
"a-stove0-opus-target"
```

### `/external_contract/cli/a-stove0-opus-target/parameters`

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

### `/external_contract/cli/a-stove0-opus-target/result_contract`

<!-- exact-contract-value: e0cbd0d9ea25b4099e3d1169c47e569308e465b4ad5cb65788d3b3afa2a96744 -->

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
  "identity": "a-stove0-opus-target-cli-result/root/v1",
  "profile_id": "a-stove0-opus-target-cli-runtime/v1",
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

### `/external_contract/cli/a-stove0-opus-target/terminating_controls`

<!-- exact-contract-value: 40fdcacd19fdeb97be50aa41342b2d7e4687a2c381fe1d87406c2fc4a19832c3 -->

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
      "distribution": "a-stove0-opus-target",
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
