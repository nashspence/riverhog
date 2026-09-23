# stove0-opus-review-sampler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-opus-review-sampler:stove0-opus-review-sampler:9e8e6b6c95 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-review-sampler](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-691e1ce527"></a>Parser name: `stove0-opus-review-sampler`
- <a id="s-56adb6a862"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-2a3fb0f176"></a>`host`<br>`--host` | optional option; 1 value | not recorded | `"127.0.0.1"` |
| <a id="s-12f56c261b"></a>`port`<br>`--port` | optional option; 1 value | int | `8080` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-2987f508e3"></a>`help` | <a id="s-338c3e26fe"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-0aaca5e9c3"></a>`0` | <a id="s-0be15b6302"></a>`"noncontractual-framework-help"` | <a id="s-a73f4effa1"></a>`"empty"` |
| <a id="s-37b9a86ef7"></a>`version` | <a id="s-d3e450ed9f"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-90613e7387"></a>`0` | <a id="s-f07a750ecd"></a>`{"distribution":"stove0-opus-review-sampler","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-c836173f33"></a>`"empty"` |

### Result and failure contract

- <a id="s-d183e7d349"></a>Result identity: `stove0-opus-review-sampler-cli-result/root/v1`
- <a id="s-db6e0ab0bd"></a>Profile: `stove0-opus-review-sampler-cli-runtime/v1`
- <a id="s-604c0d631d"></a>Structured output: `none`
- <a id="s-c5a72fbc63"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-62dd9bc223"></a>`stopped` | <a id="s-74c28b4212"></a>`{"kind":"service-runtime-returned"}` | <a id="s-c300d93a8b"></a>`0` | <a id="s-6d2ef8ca58"></a>all: `"no-command-result"` | <a id="s-53881709e6"></a>all: `"noncontractual-runtime-log"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-b574fb30a3"></a>`usage` | <a id="s-d84458c62d"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-39eb0cb98a"></a>`2` | <a id="s-42adbfb090"></a>all: `"empty"` | <a id="s-1048b10144"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --host](#s-2a3fb0f176) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --port](#s-12f56c261b) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-6bd590a18c"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-cdd0bffdaa"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-opus-review-sampler](../../../evidence/sources/authorities.md#src-77383ced4a) — [reference/stove0/targets/opus/review-sampler/src/stove0\_opus\_review\_sampler/app.py::&lt;module&gt;](../../../../../../reference/stove0/targets/opus/review-sampler/src/stove0_opus_review_sampler/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/stove0-opus-review-sampler/allow_abbrev`
- `/external_contract/cli/stove0-opus-review-sampler/name`
- `/external_contract/cli/stove0-opus-review-sampler/parameters`
- `/external_contract/cli/stove0-opus-review-sampler/result_contract`
- `/external_contract/cli/stove0-opus-review-sampler/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-opus-review-sampler/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0-opus-review-sampler/name`

<!-- exact-contract-value: 5b43d571332363c450a51914a652cb561aa5ccdf062cbcf9ebe30b08ced2da9a -->

```json
"stove0-opus-review-sampler"
```

### `/external_contract/cli/stove0-opus-review-sampler/parameters`

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

### `/external_contract/cli/stove0-opus-review-sampler/result_contract`

<!-- exact-contract-value: 9758b4bbdb18903fc46faf339ecf73a80e01469f9d61e8866d1536201949a73a -->

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
  "identity": "stove0-opus-review-sampler-cli-result/root/v1",
  "profile_id": "stove0-opus-review-sampler-cli-runtime/v1",
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

### `/external_contract/cli/stove0-opus-review-sampler/terminating_controls`

<!-- exact-contract-value: 267965eacc19d3287caa603c720eb58de90cc42b77ebc44fb8ccbd11374dddbd -->

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
      "distribution": "stove0-opus-review-sampler",
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
