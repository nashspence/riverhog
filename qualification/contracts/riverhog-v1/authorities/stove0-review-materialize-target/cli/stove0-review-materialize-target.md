# stove0-review-materialize-target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-review-materialize-target:stove0-review-materialize-target:c0183c8cb2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-materialize-target](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-3c62888a8f"></a>Parser name: `stove0-review-materialize-target`
- <a id="s-61adc05bc7"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-0732204d82"></a>`host`<br>`--host` | optional option; 1 value | not recorded | `"127.0.0.1"` |
| <a id="s-250c2a8e9e"></a>`port`<br>`--port` | optional option; 1 value | int | `8080` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-f96173b1f8"></a>`help` | <a id="s-f47b7f463a"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-ece780cc44"></a>`0` | <a id="s-8396c1528b"></a>`"noncontractual-framework-help"` | <a id="s-2361f10da9"></a>`"empty"` |
| <a id="s-07d00804ce"></a>`version` | <a id="s-c1cd159545"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-557511b3b4"></a>`0` | <a id="s-8cb8b805bf"></a>`{"distribution":"stove0-review-materialize-target","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-b7d8c6ecb6"></a>`"empty"` |

### Result and failure contract

- <a id="s-604383e5f4"></a>Result identity: `stove0-review-materialize-target-cli-result/root/v1`
- <a id="s-a47bb377dd"></a>Profile: `stove0-review-materialize-target-cli-runtime/v1`
- <a id="s-b98a300fec"></a>Structured output: `none`
- <a id="s-eb2a8b5cef"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-6baf085790"></a>`stopped` | <a id="s-e59743455a"></a>`{"kind":"service-runtime-returned"}` | <a id="s-177aaa3907"></a>`0` | <a id="s-aed8786122"></a>all: `no-command-result` | <a id="s-f71fc3f17f"></a>all: `noncontractual-runtime-log` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-2cb4856930"></a>`usage` | <a id="s-eaba7860f5"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-a7beeac2a6"></a>`2` | <a id="s-de1fb35e05"></a>all: `empty` | <a id="s-9c7d7f3106"></a>all: `noncontractual-usage-diagnostic` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --host](#s-0732204d82) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --port](#s-250c2a8e9e) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-8b88da6b6a"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-76627dfe94"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-review-materialize-target](../../../evidence/sources.md#src-7a955a3583) — `reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/app.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0-review-materialize-target/allow_abbrev`
- `/external_contract/cli/stove0-review-materialize-target/name`
- `/external_contract/cli/stove0-review-materialize-target/parameters`
- `/external_contract/cli/stove0-review-materialize-target/result_contract`
- `/external_contract/cli/stove0-review-materialize-target/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-review-materialize-target/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0-review-materialize-target/name`

<!-- exact-contract-value: 32148043a3f90bad5e4560eaae44afe7082232302760a5e5acbb4bad98f048d1 -->

```json
"stove0-review-materialize-target"
```

### `/external_contract/cli/stove0-review-materialize-target/parameters`

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

### `/external_contract/cli/stove0-review-materialize-target/result_contract`

<!-- exact-contract-value: b4c0cdefc5b6ddad9de3161a996e33f2170de48e51e773e84099832070223d1d -->

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
  "identity": "stove0-review-materialize-target-cli-result/root/v1",
  "profile_id": "stove0-review-materialize-target-cli-runtime/v1",
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

### `/external_contract/cli/stove0-review-materialize-target/terminating_controls`

<!-- exact-contract-value: cb5a00e25805ba529120a858a54ea9f92e8de7f10bb933d39ea1bb1652b2bee9 -->

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
      "distribution": "stove0-review-materialize-target",
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
