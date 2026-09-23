# a-review0-opus-sampler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-review0-opus-sampler:a-review0-opus-sampler:d64d974064 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-opus-sampler](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-2fedc7120c"></a>Parser name: `a-review0-opus-sampler`
- <a id="s-079b983d9a"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-dfe26920c9"></a>`host`<br>`--host` | optional option; 1 value | not recorded | `"127.0.0.1"` |
| <a id="s-4d8ecf8d74"></a>`port`<br>`--port` | optional option; 1 value | int | `8080` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-465a16e1d2"></a>`help` | <a id="s-5861b855ed"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-8f2ac283bb"></a>`0` | <a id="s-5d9a6e1f13"></a>`"noncontractual-framework-help"` | <a id="s-5c47f651bb"></a>`"empty"` |
| <a id="s-a8d966df78"></a>`version` | <a id="s-8794af0004"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-07d5f6839b"></a>`0` | <a id="s-673518b1fa"></a>`{"distribution":"a-review0-opus-sampler","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-a22bbd2796"></a>`"empty"` |

### Result and failure contract

- <a id="s-734955deb1"></a>Result identity: `a-review0-opus-sampler-cli-result/root/v1`
- <a id="s-d05cc73a65"></a>Profile: `a-review0-opus-sampler-cli-runtime/v1`
- <a id="s-b8e7265b62"></a>Structured output: `none`
- <a id="s-2cdcc4f34a"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-886421364d"></a>`stopped` | <a id="s-4fcc0bdf50"></a>`{"kind":"service-runtime-returned"}` | <a id="s-ea922d48a0"></a>`0` | <a id="s-bb82d6d09d"></a>all: `"no-command-result"` | <a id="s-b2daf68cf7"></a>all: `"noncontractual-runtime-log"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-4c7dfc51c8"></a>`usage` | <a id="s-c5781e1420"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-a7090d671d"></a>`2` | <a id="s-8ef2cb8777"></a>all: `"empty"` | <a id="s-281778d454"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --host](#s-dfe26920c9) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --port](#s-4d8ecf8d74) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-ac729f8e0e"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-55baed850c"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-review0-opus-sampler](../../../evidence/sources/authorities.md#src-967981bf1e) — [some-implementations/stove0/review0/samplers/opus/src/a\_review0\_opus\_sampler/app.py::&lt;module&gt;](../../../../../../some-implementations/stove0/review0/samplers/opus/src/a_review0_opus_sampler/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-review0-opus-sampler/allow_abbrev`
- `/external_contract/cli/a-review0-opus-sampler/name`
- `/external_contract/cli/a-review0-opus-sampler/parameters`
- `/external_contract/cli/a-review0-opus-sampler/result_contract`
- `/external_contract/cli/a-review0-opus-sampler/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-review0-opus-sampler/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-review0-opus-sampler/name`

<!-- exact-contract-value: 86d033876bdc9f540950e7489aebe3f8e061305c86e0a8aba9a8edb3fda8f333 -->

```json
"a-review0-opus-sampler"
```

### `/external_contract/cli/a-review0-opus-sampler/parameters`

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

### `/external_contract/cli/a-review0-opus-sampler/result_contract`

<!-- exact-contract-value: 86adb8164c5721aad0965631aa2bb89643941eae9e91114c2d5b693af2534807 -->

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
  "identity": "a-review0-opus-sampler-cli-result/root/v1",
  "profile_id": "a-review0-opus-sampler-cli-runtime/v1",
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

### `/external_contract/cli/a-review0-opus-sampler/terminating_controls`

<!-- exact-contract-value: 99f9255d2154c081d7a2c2db47b1c3e90954ed3e3e0beae8e365522250fa0f84 -->

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
      "distribution": "a-review0-opus-sampler",
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
