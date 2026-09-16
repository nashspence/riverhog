# riverhog-storage-adapter-schemas

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-storage-adapter-support:riverhog-storage-adapter-schemas:0fe77dd0d9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-1ea2979964"></a>Parser name: `riverhog-storage-adapter-schemas`

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-c7cd964cec"></a>`output`<br>`--output` | optional option; 1 value | Path | not recorded |
| <a id="s-38128e6046"></a>`compact`<br>`--compact` | optional flag; 0 values | not recorded | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-b3d710a37a"></a>`help` | <a id="s-52e00ab7aa"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-5d1501d7f2"></a>`0` | <a id="s-31282b58ad"></a>`"noncontractual-framework-help"` | <a id="s-2181ca8ce1"></a>`"empty"` |

### Result and failure contract

- <a id="s-8fd885ec44"></a>Result identity: `riverhog-storage-adapter-schemas-cli-result/root/v1`
- <a id="s-82824ba7e7"></a>Profile: `riverhog-storage-adapter-schemas-cli/v1`
- <a id="s-2c6875f114"></a>Structured output: `always-json`
- <a id="s-c9db2c71bb"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-c1a8bb97d5"></a>`emitted` | <a id="s-8bded2382d"></a>`{"kind":"option-absent","parameter":"output"}` | <a id="s-6198931a89"></a>`0` | <a id="s-4e5ccf02c2"></a>json: [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md) | <a id="s-b3f13bd5e3"></a>all: `empty` |
| <a id="s-3ce2798c93"></a>`written` | <a id="s-f819f64438"></a>`{"kind":"option-present","parameter":"output"}` | <a id="s-3920cf9c82"></a>`0` | <a id="s-3f62ca3ca0"></a>all: `empty` | <a id="s-edaca0f659"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-f6f6ad03e6"></a>`usage` | <a id="s-74d2d47cdd"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-90cc83a736"></a>`2` | <a id="s-26a045abe5"></a>all: `empty` | <a id="s-c4868a8c48"></a>all: `noncontractual-usage-diagnostic` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --output](#s-c7cd964cec) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter --compact](#s-38128e6046) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0 |

## Governing policies

- <a id="pa-a168ca8923"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-2b9982d0f7"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:riverhog-storage-adapter-schemas](../../../evidence/sources.md#src-b90a9d08ff) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/riverhog-storage-adapter-schemas/name`
- `/external_contract/cli/riverhog-storage-adapter-schemas/parameters`
- `/external_contract/cli/riverhog-storage-adapter-schemas/result_contract`
- `/external_contract/cli/riverhog-storage-adapter-schemas/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-storage-adapter-schemas/name`

<!-- exact-contract-value: 5d3570bf51282d4e0e38219724ecae45dc42a71b7197217815a8e82244b0a3d4 -->

```json
"riverhog-storage-adapter-schemas"
```

### `/external_contract/cli/riverhog-storage-adapter-schemas/parameters`

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

### `/external_contract/cli/riverhog-storage-adapter-schemas/result_contract`

<!-- exact-contract-value: f6d14ceb0402df76a98586c3d73643c76aecefd5b83f0b80760368870f499dd3 -->

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
  "identity": "riverhog-storage-adapter-schemas-cli-result/root/v1",
  "profile_id": "riverhog-storage-adapter-schemas-cli/v1",
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
          "authority": "generated:riverhog-storage-adapter",
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

### `/external_contract/cli/riverhog-storage-adapter-schemas/terminating_controls`

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
