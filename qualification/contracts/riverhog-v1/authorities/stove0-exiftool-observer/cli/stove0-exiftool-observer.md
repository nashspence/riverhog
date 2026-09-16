# stove0-exiftool-observer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-exiftool-observer:stove0-exiftool-observer:abc0f49907 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-exiftool-observer](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-c09f6374f3"></a>Parser name: `stove0-exiftool-observer`
- <a id="s-520b622a9e"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-b83ba7fb3d"></a>`host`<br>`--host` | optional option; 1 value | not recorded | `"127.0.0.1"` |
| <a id="s-e05c58ebd0"></a>`port`<br>`--port` | optional option; 1 value | int | `8080` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-f66983ae37"></a>`help` | <a id="s-55b492e4db"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-81b8d218f6"></a>`0` | <a id="s-2af1c22b0a"></a>`"noncontractual-framework-help"` | <a id="s-ddc9f367c2"></a>`"empty"` |
| <a id="s-0418ed3787"></a>`version` | <a id="s-621ef21772"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-615f4ff9d0"></a>`0` | <a id="s-d3938cd7e6"></a>`{"distribution":"stove0-exiftool-observer","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-9def224e95"></a>`"empty"` |

### Result and failure contract

- <a id="s-cb10c6c5fd"></a>Result identity: `stove0-exiftool-observer-cli-result/root/v1`
- <a id="s-15b765bfa6"></a>Profile: `stove0-exiftool-observer-cli-runtime/v1`
- <a id="s-5797f23e26"></a>Structured output: `none`
- <a id="s-4a537f0c46"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-bfd63f866d"></a>`stopped` | <a id="s-d93a409233"></a>`{"kind":"service-runtime-returned"}` | <a id="s-29f25105f4"></a>`0` | <a id="s-a5a45b8879"></a>all: `"no-command-result"` | <a id="s-b5ceba08c6"></a>all: `"noncontractual-runtime-log"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-9b2dcef678"></a>`usage` | <a id="s-42505e8c21"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-07cb266e60"></a>`2` | <a id="s-eafd598af8"></a>all: `"empty"` | <a id="s-788bfa4545"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --host](#s-b83ba7fb3d) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --port](#s-e05c58ebd0) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-96a44db515"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-c7798fb90c"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-exiftool-observer](../../../evidence/sources.md#src-55b0b4165f) — `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0-exiftool-observer/allow_abbrev`
- `/external_contract/cli/stove0-exiftool-observer/name`
- `/external_contract/cli/stove0-exiftool-observer/parameters`
- `/external_contract/cli/stove0-exiftool-observer/result_contract`
- `/external_contract/cli/stove0-exiftool-observer/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-exiftool-observer/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0-exiftool-observer/name`

<!-- exact-contract-value: 76f4d13e6d35b18da2c48018a6997d69b5617f627f6a8784d1c92be89ae55d76 -->

```json
"stove0-exiftool-observer"
```

### `/external_contract/cli/stove0-exiftool-observer/parameters`

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

### `/external_contract/cli/stove0-exiftool-observer/result_contract`

<!-- exact-contract-value: af33dfa9cee7ab8a32d1820c9c019077614eee3ffd37224c271b0a19b67b1f95 -->

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
  "identity": "stove0-exiftool-observer-cli-result/root/v1",
  "profile_id": "stove0-exiftool-observer-cli-runtime/v1",
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

### `/external_contract/cli/stove0-exiftool-observer/terminating_controls`

<!-- exact-contract-value: 5bca9fda71125167a9e72f9b5c3884e566b195a84151fcd715bce6bf58ad26c5 -->

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
      "distribution": "stove0-exiftool-observer",
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
