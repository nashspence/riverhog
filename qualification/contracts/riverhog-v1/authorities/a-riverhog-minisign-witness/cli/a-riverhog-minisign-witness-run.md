# a-riverhog-minisign-witness run

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-minisign-witness:a-riverhog-minisign-witness-run:bbaa483d83 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-minisign-witness](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-805d3c6917"></a>Parser name: `run`
- <a id="s-ca2640d030"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-a3a2e2c16b"></a>`secret_key`<br>`--secret-key` | required option; 1 value | Path | not recorded |
| <a id="s-c4a5418696"></a>`public_key`<br>`--public-key` | required option; 1 value | Path | not recorded |
| <a id="s-b10d70017b"></a>`poll_seconds`<br>`--poll-seconds` | optional option; 1 value | int | `60` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-9501f635c7"></a>`help` | <a id="s-464e09542c"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-66a4c1ee20"></a>`0` | <a id="s-588cfd98cf"></a>`"noncontractual-framework-help"` | <a id="s-e5d55cb043"></a>`"empty"` |

### Result and failure contract

- <a id="s-9a4b4cb19f"></a>Result identity: `a-riverhog-minisign-witness-cli-result/run/v1`
- <a id="s-faa27a7e88"></a>Profile: `a-riverhog-minisign-witness-cli-runtime/v1`
- <a id="s-763a2e830e"></a>Structured output: `none`
- <a id="s-88da7ab606"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-440be4c41a"></a>`runtime-returned` | <a id="s-03c9f6f63a"></a>`{"kind":"service-runtime-returned"}` | <a id="s-deb551d98f"></a>`0` | <a id="s-12edfe79f1"></a>all: `"no-command-result"` | <a id="s-155ca1723b"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-4bccfa5586"></a>`usage` | <a id="s-88d5de2538"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-ca7507f5a2"></a>`2` | <a id="s-64b0a616f9"></a>all: `"empty"` | <a id="s-ce96667c1c"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-4dc92c2813"></a>`application-error` | <a id="s-3579bd68be"></a>`{"kind":"application-error"}` | <a id="s-e51a7e3ea2"></a>`1` | <a id="s-f57b0b26b9"></a>all: `"empty"` | <a id="s-0ce39e1e60"></a>all: `"noncontractual-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --secret-key](#s-a3a2e2c16b) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --public-key](#s-c4a5418696) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --poll-seconds](#s-b10d70017b) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-4331105ebc"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-a42ac489c7"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-minisign-witness](../../../evidence/sources/authorities.md#src-ab8103c6de) — [some-implementations/riverhog/applications/a-riverhog-minisign-witness/src/a\_riverhog\_minisign\_witness/cli.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-minisign-witness/src/a_riverhog_minisign_witness/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-minisign-witness/commands/run/allow_abbrev`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/run/name`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/run/parameters`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/run/result_contract`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/run/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-minisign-witness/commands/run/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-minisign-witness/commands/run/name`

<!-- exact-contract-value: 5e87f618bd8837e87070ae7f83753c6a23ff095f43de6ededcd38ae535031c29 -->

```json
"run"
```

### `/external_contract/cli/a-riverhog-minisign-witness/commands/run/parameters`

<!-- exact-contract-value: 17b652ec11eef77c92ecb3b5e4aee58e5c92135081f4a0f09511047b298df1eb -->

```json
[
  {
    "dest": "secret_key",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--secret-key"
    ],
    "required": true,
    "type": "Path"
  },
  {
    "dest": "public_key",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--public-key"
    ],
    "required": true,
    "type": "Path"
  },
  {
    "default": 60,
    "dest": "poll_seconds",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--poll-seconds"
    ],
    "required": false,
    "type": "int"
  }
]
```

### `/external_contract/cli/a-riverhog-minisign-witness/commands/run/result_contract`

<!-- exact-contract-value: 9f9c78f03bb238fc36d7cdac4fd7925d5b95abc037cf60ad9ed30e4a99887bd0 -->

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
    },
    {
      "exit_status": 1,
      "id": "application-error",
      "selected_by": {
        "kind": "application-error"
      },
      "stderr": {
        "all": "noncontractual-diagnostic"
      },
      "stdout": {
        "all": "empty"
      }
    }
  ],
  "human_json_relationship": "not-applicable",
  "identity": "a-riverhog-minisign-witness-cli-result/run/v1",
  "profile_id": "a-riverhog-minisign-witness-cli-runtime/v1",
  "structured_output": "none",
  "success": [
    {
      "exit_status": 0,
      "id": "runtime-returned",
      "selected_by": {
        "kind": "service-runtime-returned"
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "all": "no-command-result"
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-minisign-witness/commands/run/terminating_controls`

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
