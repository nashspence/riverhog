# a-stove0-exiftool-observer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-stove0-exiftool-observer:a-stove0-exiftool-observer:ae3acb5cf0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-exiftool-observer](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-fb620d9d2b"></a>Parser name: `a-stove0-exiftool-observer`
- <a id="s-cbe6976517"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-64f910a710"></a>`host`<br>`--host` | optional option; 1 value | not recorded | `"127.0.0.1"` |
| <a id="s-c841488b7d"></a>`port`<br>`--port` | optional option; 1 value | int | `8080` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-6c74def759"></a>`help` | <a id="s-0b98e306b3"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-ecc1079ed5"></a>`0` | <a id="s-567e9ac7dc"></a>`"noncontractual-framework-help"` | <a id="s-e0be06a1bb"></a>`"empty"` |
| <a id="s-f4d985fd47"></a>`version` | <a id="s-32afce7642"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-ed9e57678d"></a>`0` | <a id="s-9daf9a5cae"></a>`{"distribution":"a-stove0-exiftool-observer","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-44b8a901c5"></a>`"empty"` |

### Result and failure contract

- <a id="s-378c68aa70"></a>Result identity: `a-stove0-exiftool-observer-cli-result/root/v1`
- <a id="s-a73c965ca6"></a>Profile: `a-stove0-exiftool-observer-cli-runtime/v1`
- <a id="s-f103733655"></a>Structured output: `none`
- <a id="s-b9fa766601"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-9613f3cb89"></a>`stopped` | <a id="s-0aa1ea14ad"></a>`{"kind":"service-runtime-returned"}` | <a id="s-0866206b6c"></a>`0` | <a id="s-2b3f0fdf27"></a>all: `"no-command-result"` | <a id="s-a92fd10904"></a>all: `"noncontractual-runtime-log"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-af8fe32ecb"></a>`usage` | <a id="s-004b92d7b0"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-b900f086b2"></a>`2` | <a id="s-8dd184eaf8"></a>all: `"empty"` | <a id="s-98b675debb"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --host](#s-64f910a710) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --port](#s-c841488b7d) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-b525803ee9"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-296791aa2d"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-stove0-exiftool-observer](../../../evidence/sources/authorities.md#src-78927b857f) — [some-implementations/stove0/observers/exiftool/src/a\_stove0\_exiftool\_observer/app.py::&lt;module&gt;](../../../../../../some-implementations/stove0/observers/exiftool/src/a_stove0_exiftool_observer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-stove0-exiftool-observer/allow_abbrev`
- `/external_contract/cli/a-stove0-exiftool-observer/name`
- `/external_contract/cli/a-stove0-exiftool-observer/parameters`
- `/external_contract/cli/a-stove0-exiftool-observer/result_contract`
- `/external_contract/cli/a-stove0-exiftool-observer/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-stove0-exiftool-observer/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-stove0-exiftool-observer/name`

<!-- exact-contract-value: ca6f6f7c15b82e35174b62a0256ec77087e4f7d11b271f236fae61e3e32a9372 -->

```json
"a-stove0-exiftool-observer"
```

### `/external_contract/cli/a-stove0-exiftool-observer/parameters`

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

### `/external_contract/cli/a-stove0-exiftool-observer/result_contract`

<!-- exact-contract-value: e45ebe3622c1b96cf061f73c8c70a52abe426f958ce0c6a1ad52b8945c3b16bc -->

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
  "identity": "a-stove0-exiftool-observer-cli-result/root/v1",
  "profile_id": "a-stove0-exiftool-observer-cli-runtime/v1",
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

### `/external_contract/cli/a-stove0-exiftool-observer/terminating_controls`

<!-- exact-contract-value: 8e1883f2bbfc0f299cc973ff07ab07bbb9f0206b47f6c209392cdad229516f09 -->

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
      "distribution": "a-stove0-exiftool-observer",
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
