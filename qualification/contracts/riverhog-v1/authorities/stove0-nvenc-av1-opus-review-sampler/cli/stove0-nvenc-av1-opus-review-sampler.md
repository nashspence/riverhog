# stove0-nvenc-av1-opus-review-sampler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-nvenc-av1-opus-review-sampler:stove0-nvenc-av1-opus-review-sampler:a6c5832001 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-review-sampler](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-121a36339a"></a>Parser name: `stove0-nvenc-av1-opus-review-sampler`
- <a id="s-39a4cfad09"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-f752c60ea3"></a>`host`<br>`--host` | optional option; 1 value | not recorded | `"127.0.0.1"` |
| <a id="s-c333f21cd3"></a>`port`<br>`--port` | optional option; 1 value | int | `8080` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-f9dd1c5113"></a>`help` | <a id="s-421d422cc1"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-e8192e4f17"></a>`0` | <a id="s-53ae8b2ab4"></a>`"noncontractual-framework-help"` | <a id="s-57bc2390f7"></a>`"empty"` |
| <a id="s-3bceb367e4"></a>`version` | <a id="s-de7891333c"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-9738957e20"></a>`0` | <a id="s-29efb8e39e"></a>`{"distribution":"stove0-nvenc-av1-opus-review-sampler","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-cfeaa1d595"></a>`"empty"` |

### Result and failure contract

- <a id="s-7ccf535641"></a>Result identity: `stove0-nvenc-av1-opus-review-sampler-cli-result/root/v1`
- <a id="s-8b8d8857bf"></a>Profile: `stove0-nvenc-av1-opus-review-sampler-cli-runtime/v1`
- <a id="s-be8971e7fd"></a>Structured output: `none`
- <a id="s-bc1c7d706c"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-5990e7bba6"></a>`stopped` | <a id="s-72b377acbe"></a>`{"kind":"service-runtime-returned"}` | <a id="s-ca4a6bcab1"></a>`0` | <a id="s-9f5953e534"></a>all: `"no-command-result"` | <a id="s-da79cdf8cf"></a>all: `"noncontractual-runtime-log"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-55de38bb50"></a>`usage` | <a id="s-95d915684d"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-1f374753a8"></a>`2` | <a id="s-fcc234584b"></a>all: `"empty"` | <a id="s-982e7e9e28"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --host](#s-f752c60ea3) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --port](#s-c333f21cd3) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-eec3b25247"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-8abffb1514"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-nvenc-av1-opus-review-sampler](../../../evidence/sources.md#src-eaca5681c1) — [reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0\_nvenc\_av1\_opus\_review\_sampler/app.py::&lt;module&gt;](../../../../../../reference/stove0/targets/nvenc-av1-opus/review-sampler/src/stove0_nvenc_av1_opus_review_sampler/app.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/stove0-nvenc-av1-opus-review-sampler/allow_abbrev`
- `/external_contract/cli/stove0-nvenc-av1-opus-review-sampler/name`
- `/external_contract/cli/stove0-nvenc-av1-opus-review-sampler/parameters`
- `/external_contract/cli/stove0-nvenc-av1-opus-review-sampler/result_contract`
- `/external_contract/cli/stove0-nvenc-av1-opus-review-sampler/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-nvenc-av1-opus-review-sampler/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0-nvenc-av1-opus-review-sampler/name`

<!-- exact-contract-value: 94a0d27d2f6ba42bd6058b511195c2ff583d5683bf397e34cfce46b5e0d3bdbe -->

```json
"stove0-nvenc-av1-opus-review-sampler"
```

### `/external_contract/cli/stove0-nvenc-av1-opus-review-sampler/parameters`

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

### `/external_contract/cli/stove0-nvenc-av1-opus-review-sampler/result_contract`

<!-- exact-contract-value: 647d8853b7375baad63635a366610b9413daaef95e08f1b138291dea052f133d -->

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
  "identity": "stove0-nvenc-av1-opus-review-sampler-cli-result/root/v1",
  "profile_id": "stove0-nvenc-av1-opus-review-sampler-cli-runtime/v1",
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

### `/external_contract/cli/stove0-nvenc-av1-opus-review-sampler/terminating_controls`

<!-- exact-contract-value: 3d38ea7b2cb8b4bce5624b54792c17ec27066afbded6d62da7f83aab9304f7d6 -->

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
      "distribution": "stove0-nvenc-av1-opus-review-sampler",
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
