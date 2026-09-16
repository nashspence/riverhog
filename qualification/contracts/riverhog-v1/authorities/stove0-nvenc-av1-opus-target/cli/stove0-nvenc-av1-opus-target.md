# stove0-nvenc-av1-opus-target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-nvenc-av1-opus-target:stove0-nvenc-av1-opus-target:2186537e2f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-6139065804"></a>Parser name: `stove0-nvenc-av1-opus-target`
- <a id="s-11f4f9d52a"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-93333f3d07"></a>`host`<br>`--host` | optional option; 1 value | not recorded | `"127.0.0.1"` |
| <a id="s-41e018dc47"></a>`port`<br>`--port` | optional option; 1 value | int | `8080` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-5ef26ae6c8"></a>`help` | <a id="s-8d79ef8c0a"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-a9961e1648"></a>`0` | <a id="s-72a069022a"></a>`"noncontractual-framework-help"` | <a id="s-5d5cd92e8d"></a>`"empty"` |
| <a id="s-4cde3d9b0d"></a>`version` | <a id="s-1aaa93fe8d"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-938a4515c0"></a>`0` | <a id="s-c011e8621c"></a>`{"distribution":"stove0-nvenc-av1-opus-target","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-935f0e373d"></a>`"empty"` |

### Result and failure contract

- <a id="s-1968ef34c1"></a>Result identity: `stove0-nvenc-av1-opus-target-cli-result/root/v1`
- <a id="s-c2a51172d9"></a>Profile: `stove0-nvenc-av1-opus-target-cli-runtime/v1`
- <a id="s-98b70df96c"></a>Structured output: `none`
- <a id="s-af212735df"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-26fae00a15"></a>`stopped` | <a id="s-b0f4e470e4"></a>`{"kind":"service-runtime-returned"}` | <a id="s-f1cad057df"></a>`0` | <a id="s-f2acd0831c"></a>all: `"no-command-result"` | <a id="s-dcb029a670"></a>all: `"noncontractual-runtime-log"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-914a7c1a99"></a>`usage` | <a id="s-44fc7a1b61"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-34d1eb7cc4"></a>`2` | <a id="s-1c913bde44"></a>all: `"empty"` | <a id="s-0978b2b542"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --host](#s-93333f3d07) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --port](#s-41e018dc47) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-a0a41bd094"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-0b61330c50"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-nvenc-av1-opus-target](../../../evidence/sources.md#src-f133946353) — `reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/app.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0-nvenc-av1-opus-target/allow_abbrev`
- `/external_contract/cli/stove0-nvenc-av1-opus-target/name`
- `/external_contract/cli/stove0-nvenc-av1-opus-target/parameters`
- `/external_contract/cli/stove0-nvenc-av1-opus-target/result_contract`
- `/external_contract/cli/stove0-nvenc-av1-opus-target/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-nvenc-av1-opus-target/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0-nvenc-av1-opus-target/name`

<!-- exact-contract-value: 88c1291a7263e19a2cc7c99e5623e9637286311feacba21b3db74b531c03abc1 -->

```json
"stove0-nvenc-av1-opus-target"
```

### `/external_contract/cli/stove0-nvenc-av1-opus-target/parameters`

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

### `/external_contract/cli/stove0-nvenc-av1-opus-target/result_contract`

<!-- exact-contract-value: 4f034e390a6af6d81d66eeb1512ef67a3d9a7d90825ad679c39176af749bb720 -->

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
  "identity": "stove0-nvenc-av1-opus-target-cli-result/root/v1",
  "profile_id": "stove0-nvenc-av1-opus-target-cli-runtime/v1",
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

### `/external_contract/cli/stove0-nvenc-av1-opus-target/terminating_controls`

<!-- exact-contract-value: b07cbf8eb2b388cb488791b54807eaddd64c1b45f2f553acfcacc3821248d260 -->

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
      "distribution": "stove0-nvenc-av1-opus-target",
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
