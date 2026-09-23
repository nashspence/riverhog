# a-riverhog-filesystem-store

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-filesystem-store:a-riverhog-filesystem-store:4bf9d11118 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-filesystem-store](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-238bc85657"></a>Parser name: `a-riverhog-filesystem-store`
- <a id="s-dd4499baa6"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-5b52322f7a"></a>`host`<br>`--host` | optional option; 1 value | not recorded | `"127.0.0.1"` |
| <a id="s-07505e076f"></a>`port`<br>`--port` | optional option; 1 value | int | `8080` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-944fb93ee6"></a>`help` | <a id="s-2865842f89"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-df9ff4c195"></a>`0` | <a id="s-d8d4064e4b"></a>`"noncontractual-framework-help"` | <a id="s-f97bdc667d"></a>`"empty"` |
| <a id="s-f39a81d4ce"></a>`version` | <a id="s-78cfe6840e"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-838a24567c"></a>`0` | <a id="s-dd294c0793"></a>`{"distribution":"a-riverhog-filesystem-store","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-72c6ed4f2f"></a>`"empty"` |

### Result and failure contract

- <a id="s-cc9e656859"></a>Result identity: `a-riverhog-filesystem-store-cli-result/root/v1`
- <a id="s-2bcbd319a5"></a>Profile: `a-riverhog-filesystem-store-cli-runtime/v1`
- <a id="s-28462ffea3"></a>Structured output: `none`
- <a id="s-debafc0523"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-2b7c387ca4"></a>`stopped` | <a id="s-723d5ef5cd"></a>`{"kind":"service-runtime-returned"}` | <a id="s-3d68e3765a"></a>`0` | <a id="s-ca83e5561d"></a>all: `"no-command-result"` | <a id="s-ed853bd37c"></a>all: `"noncontractual-runtime-log"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-5ec8b6a6f7"></a>`usage` | <a id="s-ee53ee24b0"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-a0e3f39f88"></a>`2` | <a id="s-7dda2227d2"></a>all: `"empty"` | <a id="s-3567982484"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --host](#s-5b52322f7a) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --port](#s-07505e076f) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-a8e088804f"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-fbb9880eb6"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-filesystem-store](../../../evidence/sources/authorities.md#src-9e1b603774) — [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/app.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-filesystem-store/allow_abbrev`
- `/external_contract/cli/a-riverhog-filesystem-store/name`
- `/external_contract/cli/a-riverhog-filesystem-store/parameters`
- `/external_contract/cli/a-riverhog-filesystem-store/result_contract`
- `/external_contract/cli/a-riverhog-filesystem-store/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-filesystem-store/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-filesystem-store/name`

<!-- exact-contract-value: 0d49720e0f0b68a182e645939aa10c099adad2a42cb161b81c1083f0b3eb2382 -->

```json
"a-riverhog-filesystem-store"
```

### `/external_contract/cli/a-riverhog-filesystem-store/parameters`

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

### `/external_contract/cli/a-riverhog-filesystem-store/result_contract`

<!-- exact-contract-value: 6f45d3e8fae06b47dbcc5f93a7c31b434a648774ee219895a737382eff7caccc -->

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
  "identity": "a-riverhog-filesystem-store-cli-result/root/v1",
  "profile_id": "a-riverhog-filesystem-store-cli-runtime/v1",
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

### `/external_contract/cli/a-riverhog-filesystem-store/terminating_controls`

<!-- exact-contract-value: d4b2bd5f29d6109a5c1bce0eb5ada925b9e3b4b35d0d31cbb34713cee68295b5 -->

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
      "distribution": "a-riverhog-filesystem-store",
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
