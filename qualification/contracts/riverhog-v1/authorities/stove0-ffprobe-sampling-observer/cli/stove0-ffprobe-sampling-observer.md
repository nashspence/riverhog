# stove0-ffprobe-sampling-observer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-ffprobe-sampling-observer:stove0-ffprobe-sampling-observer:d311d2d324 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-f09114da6d"></a>Parser name: `stove0-ffprobe-sampling-observer`
- <a id="s-46f6e207ef"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-4520d13002"></a>`host`<br>`--host` | optional option; 1 value | not recorded | `"127.0.0.1"` |
| <a id="s-80dfc103de"></a>`port`<br>`--port` | optional option; 1 value | int | `8080` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-1975f9b261"></a>`help` | <a id="s-8a775dfe91"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-81fdec68eb"></a>`0` | <a id="s-7ddf180607"></a>`"noncontractual-framework-help"` | <a id="s-bbeae076d4"></a>`"empty"` |
| <a id="s-6ba23c9e60"></a>`version` | <a id="s-e9b4490a69"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-15f494da64"></a>`0` | <a id="s-59112e1400"></a>`{"distribution":"stove0-ffprobe-sampling-observer","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-3e35845f86"></a>`"empty"` |

### Result and failure contract

- <a id="s-6dc418caf6"></a>Result identity: `stove0-ffprobe-sampling-observer-cli-result/root/v1`
- <a id="s-c03c22a8e2"></a>Profile: `stove0-ffprobe-sampling-observer-cli-runtime/v1`
- <a id="s-6676cd906b"></a>Structured output: `none`
- <a id="s-f5fa6b5f03"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-f0951c9543"></a>`stopped` | <a id="s-20a3731af8"></a>`{"kind":"service-runtime-returned"}` | <a id="s-2566cea36a"></a>`0` | <a id="s-e22ff2bfdb"></a>all: `"no-command-result"` | <a id="s-72d7b088c0"></a>all: `"noncontractual-runtime-log"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-938437eff2"></a>`usage` | <a id="s-a388dd7d78"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-82bd63719c"></a>`2` | <a id="s-5140a15acd"></a>all: `"empty"` | <a id="s-93c38e944b"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --host](#s-4520d13002) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --port](#s-80dfc103de) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-943b7b4f9f"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-13ccef23c2"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-ffprobe-sampling-observer](../../../evidence/sources/authorities.md#src-15564fb4b2) — [reference/stove0/observers/ffprobe-sampling/src/stove0\_ffprobe\_sampling\_observer/app.py::&lt;module&gt;](../../../../../../reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/stove0-ffprobe-sampling-observer/allow_abbrev`
- `/external_contract/cli/stove0-ffprobe-sampling-observer/name`
- `/external_contract/cli/stove0-ffprobe-sampling-observer/parameters`
- `/external_contract/cli/stove0-ffprobe-sampling-observer/result_contract`
- `/external_contract/cli/stove0-ffprobe-sampling-observer/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-ffprobe-sampling-observer/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0-ffprobe-sampling-observer/name`

<!-- exact-contract-value: fbac8ad97c3757122117d79be712b543167285e0f7b5ce5ca81e04af217689c2 -->

```json
"stove0-ffprobe-sampling-observer"
```

### `/external_contract/cli/stove0-ffprobe-sampling-observer/parameters`

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

### `/external_contract/cli/stove0-ffprobe-sampling-observer/result_contract`

<!-- exact-contract-value: 0c30248241a82b0134b1fc72b657a829608d495906bdcd56062e468ef7ea77b1 -->

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
  "identity": "stove0-ffprobe-sampling-observer-cli-result/root/v1",
  "profile_id": "stove0-ffprobe-sampling-observer-cli-runtime/v1",
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

### `/external_contract/cli/stove0-ffprobe-sampling-observer/terminating_controls`

<!-- exact-contract-value: f4131cb5edbea40b583ad93dc26ff6916972ae5e2017f2801d73d1bcf6f64e5e -->

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
      "distribution": "stove0-ffprobe-sampling-observer",
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
