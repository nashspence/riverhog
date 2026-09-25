# a-riverhog-b2-store

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-b2-store:a-riverhog-b2-store:8c43bdea50 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-b2-store](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-6c72675b83"></a>Parser name: `a-riverhog-b2-store`
- <a id="s-7403ff3fe4"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-abd61975de"></a>`host`<br>`--host` | optional option; 1 value | not recorded | `"127.0.0.1"` |
| <a id="s-82838a6d98"></a>`port`<br>`--port` | optional option; 1 value | int | `8080` |
| <a id="s-086a4326f8"></a>`provision_root`<br>`--provision-root` | optional flag; 0 values | not recorded | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-4205f6aa08"></a>`help` | <a id="s-5d8d4a2e4a"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-9cff95b152"></a>`0` | <a id="s-e9f95e722a"></a>`"noncontractual-framework-help"` | <a id="s-f71d513704"></a>`"empty"` |
| <a id="s-664dfd01ce"></a>`version` | <a id="s-4fadc2678d"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-957239bd60"></a>`0` | <a id="s-43d5d74ce5"></a>`{"distribution":"a-riverhog-b2-store","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-5401f149ac"></a>`"empty"` |

### Result and failure contract

- <a id="s-ec0eb8af48"></a>Result identity: `a-riverhog-b2-store-cli-result/root/v1`
- <a id="s-b28f2c4bf1"></a>Profile: `a-riverhog-b2-store-cli-runtime/v1`
- <a id="s-09b90ed5c3"></a>Structured output: `none`
- <a id="s-52f34f6228"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-c9ae1ce875"></a>`stopped` | <a id="s-b35aa00a99"></a>`{"kind":"service-runtime-returned"}` | <a id="s-3ed8fb831c"></a>`0` | <a id="s-6171afc851"></a>all: `"no-command-result"` | <a id="s-5bbf97cbcc"></a>all: `"noncontractual-runtime-log"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-37b99a0bb9"></a>`usage` | <a id="s-fdf4051b12"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-64a679fdce"></a>`2` | <a id="s-a0ecc843c6"></a>all: `"empty"` | <a id="s-e0645e4399"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --host](#s-abd61975de) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter --port](#s-82838a6d98) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter --provision-root](#s-086a4326f8) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0 |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-838ab7aebd"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-ff885e29ef"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-b2-store](../../../evidence/sources/authorities.md#src-f977d89a7d) — [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-b2-store/allow_abbrev`
- `/external_contract/cli/a-riverhog-b2-store/name`
- `/external_contract/cli/a-riverhog-b2-store/parameters`
- `/external_contract/cli/a-riverhog-b2-store/result_contract`
- `/external_contract/cli/a-riverhog-b2-store/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-b2-store/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-b2-store/name`

<!-- exact-contract-value: ff42313f7c2d9307b0c25c2ab47412adc3c6305a902aec3f2617880de6ca3c42 -->

```json
"a-riverhog-b2-store"
```

### `/external_contract/cli/a-riverhog-b2-store/parameters`

<!-- exact-contract-value: 2e7bfbe785f76e807173a7daf67d956193711ab9464e7a8d9054a8340b54b804 -->

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
  },
  {
    "default": false,
    "dest": "provision_root",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--provision-root"
    ],
    "required": false
  }
]
```

### `/external_contract/cli/a-riverhog-b2-store/result_contract`

<!-- exact-contract-value: eeaed286df8dbce0c52eaab0d4a8be9a13aeb84d0972b9a606a7f56a6b186b7e -->

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
  "identity": "a-riverhog-b2-store-cli-result/root/v1",
  "profile_id": "a-riverhog-b2-store-cli-runtime/v1",
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

### `/external_contract/cli/a-riverhog-b2-store/terminating_controls`

<!-- exact-contract-value: 5341dfa5446e06c88a1de2718b4b7fb3984a1b51bcd99067688564582b04b94d -->

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
      "distribution": "a-riverhog-b2-store",
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
