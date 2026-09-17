# stove0-opus-target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-opus-target:stove0-opus-target:fa94685927 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-target](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-572c77475e"></a>Parser name: `stove0-opus-target`
- <a id="s-4398efa208"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-fadf4911ea"></a>`host`<br>`--host` | optional option; 1 value | not recorded | `"127.0.0.1"` |
| <a id="s-c655c66683"></a>`port`<br>`--port` | optional option; 1 value | int | `8080` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-516fdbb001"></a>`help` | <a id="s-112bd8d3c9"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-12108f6dbc"></a>`0` | <a id="s-19a736b076"></a>`"noncontractual-framework-help"` | <a id="s-f33e6d818e"></a>`"empty"` |
| <a id="s-55ddf2156d"></a>`version` | <a id="s-38a95748aa"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-5556b3cae4"></a>`0` | <a id="s-eb1b95a96c"></a>`{"distribution":"stove0-opus-target","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-3a4b03638f"></a>`"empty"` |

### Result and failure contract

- <a id="s-734ee035c2"></a>Result identity: `stove0-opus-target-cli-result/root/v1`
- <a id="s-e25146bdbf"></a>Profile: `stove0-opus-target-cli-runtime/v1`
- <a id="s-d12b693f2e"></a>Structured output: `none`
- <a id="s-34032f84ce"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-846aff8755"></a>`stopped` | <a id="s-84535142f2"></a>`{"kind":"service-runtime-returned"}` | <a id="s-7cce3758dc"></a>`0` | <a id="s-e794e76b90"></a>all: `"no-command-result"` | <a id="s-d333e8fd81"></a>all: `"noncontractual-runtime-log"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-78e55053cc"></a>`usage` | <a id="s-2c61f1d379"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-d8a0f6add9"></a>`2` | <a id="s-e981bade68"></a>all: `"empty"` | <a id="s-0ee55a1a3a"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --host](#s-fadf4911ea) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --port](#s-c655c66683) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-9bf8261659"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-de1bcffc27"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-opus-target](../../../evidence/sources/authorities.md#src-d300688475) — [reference/stove0/targets/opus/target/src/stove0\_opus\_target/app.py::&lt;module&gt;](../../../../../../reference/stove0/targets/opus/target/src/stove0_opus_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/stove0-opus-target/allow_abbrev`
- `/external_contract/cli/stove0-opus-target/name`
- `/external_contract/cli/stove0-opus-target/parameters`
- `/external_contract/cli/stove0-opus-target/result_contract`
- `/external_contract/cli/stove0-opus-target/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-opus-target/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0-opus-target/name`

<!-- exact-contract-value: 09d670b77c0ec557d0df0c81838a494b7648331bb42f83b21f2844cd19116290 -->

```json
"stove0-opus-target"
```

### `/external_contract/cli/stove0-opus-target/parameters`

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

### `/external_contract/cli/stove0-opus-target/result_contract`

<!-- exact-contract-value: 50424cc328a44d9abffd4745fc630d626b97344350c283b81637a36d505c8366 -->

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
  "identity": "stove0-opus-target-cli-result/root/v1",
  "profile_id": "stove0-opus-target-cli-runtime/v1",
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

### `/external_contract/cli/stove0-opus-target/terminating_controls`

<!-- exact-contract-value: 9c9cf8d6259eedfdc59cede7d720c64e94001c49faf226cd14aa151b61de1cf7 -->

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
      "distribution": "stove0-opus-target",
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
