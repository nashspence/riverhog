# a-review0-nvenc-av1-opus-sampler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-review0-nvenc-av1-opus-sampler:a-review0-nvenc-av1-opus-sampler:72df1a6659 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-nvenc-av1-opus-sampler](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-097743c2ad"></a>Parser name: `a-review0-nvenc-av1-opus-sampler`
- <a id="s-ab717c44f4"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-856d3d2300"></a>`host`<br>`--host` | optional option; 1 value | not recorded | `"127.0.0.1"` |
| <a id="s-43a09a46c7"></a>`port`<br>`--port` | optional option; 1 value | int | `8080` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-f50eefe099"></a>`help` | <a id="s-fbe914cb5d"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-2c4ea0ed6c"></a>`0` | <a id="s-7353f28268"></a>`"noncontractual-framework-help"` | <a id="s-f7f5e2b3c3"></a>`"empty"` |
| <a id="s-9cb8c0ac7c"></a>`version` | <a id="s-8eca566b7d"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-47b342432a"></a>`0` | <a id="s-7e3c424a40"></a>`{"distribution":"a-review0-nvenc-av1-opus-sampler","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-4b0d5ae4bd"></a>`"empty"` |

### Result and failure contract

- <a id="s-dd134fd864"></a>Result identity: `a-review0-nvenc-av1-opus-sampler-cli-result/root/v1`
- <a id="s-e9dd7beaac"></a>Profile: `a-review0-nvenc-av1-opus-sampler-cli-runtime/v1`
- <a id="s-5178bf6531"></a>Structured output: `none`
- <a id="s-14548070a5"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-7b2aa1160d"></a>`stopped` | <a id="s-c67aa9d313"></a>`{"kind":"service-runtime-returned"}` | <a id="s-becdc36bfb"></a>`0` | <a id="s-95dd9bd272"></a>all: `"no-command-result"` | <a id="s-74c41257f2"></a>all: `"noncontractual-runtime-log"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-d0efcdc3ed"></a>`usage` | <a id="s-b607453a74"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-f2f70382f7"></a>`2` | <a id="s-b6a422832e"></a>all: `"empty"` | <a id="s-58c1bdad32"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --host](#s-856d3d2300) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --port](#s-43a09a46c7) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-04a97810e5"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-49d1732f33"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-review0-nvenc-av1-opus-sampler](../../../evidence/sources/authorities.md#src-6c2edcad68) — [some-implementations/stove0/review0/samplers/nvenc-av1-opus/src/a\_review0\_nvenc\_av1\_opus\_sampler/app.py::&lt;module&gt;](../../../../../../some-implementations/stove0/review0/samplers/nvenc-av1-opus/src/a_review0_nvenc_av1_opus_sampler/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-review0-nvenc-av1-opus-sampler/allow_abbrev`
- `/external_contract/cli/a-review0-nvenc-av1-opus-sampler/name`
- `/external_contract/cli/a-review0-nvenc-av1-opus-sampler/parameters`
- `/external_contract/cli/a-review0-nvenc-av1-opus-sampler/result_contract`
- `/external_contract/cli/a-review0-nvenc-av1-opus-sampler/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-review0-nvenc-av1-opus-sampler/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-review0-nvenc-av1-opus-sampler/name`

<!-- exact-contract-value: 7e82af6f8e238a327c5c34406b38c88466fc82bb52ac04baf00f387387857964 -->

```json
"a-review0-nvenc-av1-opus-sampler"
```

### `/external_contract/cli/a-review0-nvenc-av1-opus-sampler/parameters`

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

### `/external_contract/cli/a-review0-nvenc-av1-opus-sampler/result_contract`

<!-- exact-contract-value: 78f3830a8ffae1eea02dd4e66cad0ece83f44ca3eec0e3033727e6b576509e7e -->

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
  "identity": "a-review0-nvenc-av1-opus-sampler-cli-result/root/v1",
  "profile_id": "a-review0-nvenc-av1-opus-sampler-cli-runtime/v1",
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

### `/external_contract/cli/a-review0-nvenc-av1-opus-sampler/terminating_controls`

<!-- exact-contract-value: 468853661959dbef9b1c6dfad2b3d54ba34c6d198a74c721a589f8eee59ae4e6 -->

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
      "distribution": "a-review0-nvenc-av1-opus-sampler",
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
