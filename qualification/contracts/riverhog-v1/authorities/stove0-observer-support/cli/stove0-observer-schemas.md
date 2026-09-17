# stove0-observer-schemas

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-observer-support:stove0-observer-schemas:37a6f2c18b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-b8d24568a3"></a>Parser name: `stove0-observer-schemas`
- <a id="s-7d03e3b17f"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-6c05c76d4d"></a>`output`<br>`--output` | optional option; 1 value | Path | not recorded |
| <a id="s-6ff6ce6761"></a>`compact`<br>`--compact` | optional flag; 0 values | not recorded | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-e9063f5b6c"></a>`help` | <a id="s-251a1a6377"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-add6b8f7d1"></a>`0` | <a id="s-7cf8444496"></a>`"noncontractual-framework-help"` | <a id="s-091c547c04"></a>`"empty"` |

### Result and failure contract

- <a id="s-8bb2d8e2ef"></a>Result identity: `stove0-observer-schemas-cli-result/root/v1`
- <a id="s-17bc1df9b8"></a>Profile: `stove0-observer-schemas-cli/v1`
- <a id="s-c8948cfde3"></a>Structured output: `always-json`
- <a id="s-402beb7349"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-aec3abea41"></a>`emitted` | <a id="s-9db66ee58f"></a>`{"kind":"option-absent","parameter":"output"}` | <a id="s-942cd6dca7"></a>`0` | <a id="s-4d0cbf1ffb"></a>json: [generated:stove0-observer protocol](../process-protocol/generated-stove0-observer-protocol.md) | <a id="s-662f32dea5"></a>all: `"empty"` |
| <a id="s-69f512bf20"></a>`written` | <a id="s-8ae887f5fe"></a>`{"kind":"option-present","parameter":"output"}` | <a id="s-0f78699ef3"></a>`0` | <a id="s-2880b7e871"></a>all: `"empty"` | <a id="s-dca9ab393e"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-880569c723"></a>`usage` | <a id="s-cdfa373dc4"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-c2966577a6"></a>`2` | <a id="s-b3b261f2b1"></a>all: `"empty"` | <a id="s-bba39052be"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --output](#s-6c05c76d4d) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter --compact](#s-6ff6ce6761) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0 |

## Governing policies

- <a id="pa-3d85929844"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-4b56d463bb"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-observer-schemas](../../../evidence/sources/authorities.md#src-e6175e3ae2) — [reference/stove0/packages/observer-support/src/stove0\_observer\_support/schemas.py::&lt;module&gt;](../../../../../../reference/stove0/packages/observer-support/src/stove0_observer_support/schemas.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/stove0-observer-schemas/allow_abbrev`
- `/external_contract/cli/stove0-observer-schemas/name`
- `/external_contract/cli/stove0-observer-schemas/parameters`
- `/external_contract/cli/stove0-observer-schemas/result_contract`
- `/external_contract/cli/stove0-observer-schemas/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-observer-schemas/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0-observer-schemas/name`

<!-- exact-contract-value: 39b4b08f24b8bf64cfcb76e91c23b866c91276b8e46a6d44e0a83af9f5f646e3 -->

```json
"stove0-observer-schemas"
```

### `/external_contract/cli/stove0-observer-schemas/parameters`

<!-- exact-contract-value: d09dd1e324eea493304f34cc58b3f5fe965bfdb99ef580493ab2779a9afcca7d -->

```json
[
  {
    "dest": "output",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--output"
    ],
    "required": false,
    "type": "Path"
  },
  {
    "default": false,
    "dest": "compact",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--compact"
    ],
    "required": false
  }
]
```

### `/external_contract/cli/stove0-observer-schemas/result_contract`

<!-- exact-contract-value: e4c56c4220f9d15d55d9e33042c7d7a12557a052a47a5ee8a64871b43e2adc43 -->

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
  "identity": "stove0-observer-schemas-cli-result/root/v1",
  "profile_id": "stove0-observer-schemas-cli/v1",
  "structured_output": "always-json",
  "success": [
    {
      "exit_status": 0,
      "id": "emitted",
      "selected_by": {
        "kind": "option-absent",
        "parameter": "output"
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "json": {
          "authority": "generated:stove0-observer",
          "kind": "document-authority"
        }
      }
    },
    {
      "exit_status": 0,
      "id": "written",
      "selected_by": {
        "kind": "option-present",
        "parameter": "output"
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "all": "empty"
      }
    }
  ]
}
```

### `/external_contract/cli/stove0-observer-schemas/terminating_controls`

<!-- exact-contract-value: 46c96c22d2ed8a51da57bba3ac0f2269bb35f98c5fd6dc3e3ba67e60f772ad72 -->

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
  }
]
```

</details>
