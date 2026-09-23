# a-stove0-ffprobe-sampling-observer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-stove0-ffprobe-sampling-observer:a-stove0-ffprobe-sampling-observer:67593e56b3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-459087828e"></a>Parser name: `a-stove0-ffprobe-sampling-observer`
- <a id="s-a9f4865b1d"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-b08fae23e6"></a>`host`<br>`--host` | optional option; 1 value | not recorded | `"127.0.0.1"` |
| <a id="s-f12c06306c"></a>`port`<br>`--port` | optional option; 1 value | int | `8080` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-bc47fea236"></a>`help` | <a id="s-d46c7e0a82"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-607efe4f33"></a>`0` | <a id="s-748d19a8ab"></a>`"noncontractual-framework-help"` | <a id="s-65f6e10d57"></a>`"empty"` |
| <a id="s-09d8925540"></a>`version` | <a id="s-b6a0fc1cb9"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-ba8f264f96"></a>`0` | <a id="s-f460e2e369"></a>`{"distribution":"a-stove0-ffprobe-sampling-observer","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-a80660fa5e"></a>`"empty"` |

### Result and failure contract

- <a id="s-2e9abc96f2"></a>Result identity: `a-stove0-ffprobe-sampling-observer-cli-result/root/v1`
- <a id="s-9e31ef879b"></a>Profile: `a-stove0-ffprobe-sampling-observer-cli-runtime/v1`
- <a id="s-452f3a69ac"></a>Structured output: `none`
- <a id="s-28664cd3cf"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-14801c3336"></a>`stopped` | <a id="s-1ae984f168"></a>`{"kind":"service-runtime-returned"}` | <a id="s-e2e6e7a10f"></a>`0` | <a id="s-5b7e90de8e"></a>all: `"no-command-result"` | <a id="s-a25609fa35"></a>all: `"noncontractual-runtime-log"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-e4c1cd7d32"></a>`usage` | <a id="s-d2cdc1dbc3"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-bd6ac67e1d"></a>`2` | <a id="s-6b964aab68"></a>all: `"empty"` | <a id="s-2f9f1e3904"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --host](#s-b08fae23e6) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --port](#s-f12c06306c) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-e5a8928ad5"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-84ac239db1"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-stove0-ffprobe-sampling-observer](../../../evidence/sources/authorities.md#src-04e0d7d089) — [some-implementations/stove0/observers/ffprobe-sampling/src/a\_stove0\_ffprobe\_sampling\_observer/app.py::&lt;module&gt;](../../../../../../some-implementations/stove0/observers/ffprobe-sampling/src/a_stove0_ffprobe_sampling_observer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-stove0-ffprobe-sampling-observer/allow_abbrev`
- `/external_contract/cli/a-stove0-ffprobe-sampling-observer/name`
- `/external_contract/cli/a-stove0-ffprobe-sampling-observer/parameters`
- `/external_contract/cli/a-stove0-ffprobe-sampling-observer/result_contract`
- `/external_contract/cli/a-stove0-ffprobe-sampling-observer/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-stove0-ffprobe-sampling-observer/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-stove0-ffprobe-sampling-observer/name`

<!-- exact-contract-value: c22225f377050dc9b851f8ec05e84f2bee2e5ff2a3ff0e74d9631b576a073d42 -->

```json
"a-stove0-ffprobe-sampling-observer"
```

### `/external_contract/cli/a-stove0-ffprobe-sampling-observer/parameters`

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

### `/external_contract/cli/a-stove0-ffprobe-sampling-observer/result_contract`

<!-- exact-contract-value: cdaba88260b25efa89c923f957a2bd2c6fab73dc90a79cf0bb3fe9f458ef1760 -->

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
  "identity": "a-stove0-ffprobe-sampling-observer-cli-result/root/v1",
  "profile_id": "a-stove0-ffprobe-sampling-observer-cli-runtime/v1",
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

### `/external_contract/cli/a-stove0-ffprobe-sampling-observer/terminating_controls`

<!-- exact-contract-value: c167af4000a19b5bc661e470b614a8923f94ceaad9539755c55435e7340d5580 -->

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
      "distribution": "a-stove0-ffprobe-sampling-observer",
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
