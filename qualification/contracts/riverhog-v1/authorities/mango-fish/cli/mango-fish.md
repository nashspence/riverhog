# mango-fish

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:mango-fish:mango-fish:739a779c7e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [mango-fish](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-8e837eac2f"></a>Parser name: `mango-fish`
- <a id="s-7ccec0513a"></a>Subcommand selection: optional.
- <a id="s-e49100df2e"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-1368f192cf"></a>`config`<br>`--config` | required option; 1 value | Path | not recorded |
| <a id="s-b3bbac485d"></a>`check`<br>`--check` | optional flag; 0 values | not recorded | `false` |
| <a id="s-55e6dc25a4"></a>`once`<br>`--once` | optional flag; 0 values | not recorded | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-6d3ddd4fea"></a>`help` | <a id="s-69ef5bad0f"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-7661109a30"></a>`0` | <a id="s-40a28d8a15"></a>`"noncontractual-framework-help"` | <a id="s-c1f516d4b0"></a>`"empty"` |
| <a id="s-520dfcd066"></a>`version` | <a id="s-fb70f702fb"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-ff03ca9aea"></a>`0` | <a id="s-c321d0fa54"></a>`{"distribution":"mango-fish","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-7b38225c48"></a>`"empty"` |

### Result and failure contract

- <a id="s-604d4c4bd9"></a>Result identity: `mango-fish-cli-result/root/v1`
- <a id="s-64173d57e0"></a>Profile: `mango-fish-cli-relay-runtime/v1`
- <a id="s-d637f6e309"></a>Structured output: `mode-specific`
- <a id="s-dae04cf130"></a>Human/JSON relationship: `mode-specific-results`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-c5c84fd8e9"></a>`configuration-check` | <a id="s-962fda80ff"></a>`{"kind":"option-equals","parameter":"check","value":true}` | <a id="s-a0dd530aa6"></a>`0` | <a id="s-1c5b2334bf"></a>all: `"mango-fish-configuration-summary/v1"` | <a id="s-0d4de0c37a"></a>all: `"noncontractual-runtime-log-or-empty"` |
| <a id="s-e4784b90bb"></a>`relay-completed` | <a id="s-50e91c7b08"></a>`{"kind":"option-equals","parameter":"check","value":false}` | <a id="s-22cd70bc63"></a>`0` | <a id="s-09536f2b2c"></a>all: `"no-command-result"` | <a id="s-9bfa84e578"></a>all: `"noncontractual-runtime-log-or-empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-cdbaee25f6"></a>`usage` | <a id="s-f10753be16"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-c4a4f0a60b"></a>`2` | <a id="s-beb6fde10b"></a>all: `"empty"` | <a id="s-dcf9ebe4b8"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-e215ef5c5f"></a>`relay-pass-failed` | <a id="s-0f5436649c"></a>`{"kind":"relay-pass-reported-failures"}` | <a id="s-aef55108b0"></a>`1` | <a id="s-33ac94278e"></a>all: `"no-command-result"` | <a id="s-0469ca77fd"></a>all: `"noncontractual-runtime-log"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --config](#s-1368f192cf) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1 |
| [CLI parameter --check](#s-b3bbac485d) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0 |
| [CLI parameter --once](#s-55e6dc25a4) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0 |

## Governing policies

- <a id="pa-833cbd0fc1"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-022c85e1ff"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:mango-fish](../../../evidence/sources.md#src-3dcd5eedf2) — `reference/riverhog/applications/mango-fish/src/mango_fish/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/mango-fish/allow_abbrev`
- `/external_contract/cli/mango-fish/name`
- `/external_contract/cli/mango-fish/parameters`
- `/external_contract/cli/mango-fish/result_contract`
- `/external_contract/cli/mango-fish/subcommand_required`
- `/external_contract/cli/mango-fish/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/mango-fish/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/mango-fish/name`

<!-- exact-contract-value: 1597bf55bb7889ac7479259c9f4631adf1e2188403282e8d82efa49b1815e160 -->

```json
"mango-fish"
```

### `/external_contract/cli/mango-fish/parameters`

<!-- exact-contract-value: b096906e4d3ad7dfc114a853c3f02bf4cc135bc3f5843f13cfd8572456089113 -->

```json
[
  {
    "dest": "config",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--config"
    ],
    "required": true,
    "type": "Path"
  },
  {
    "default": false,
    "dest": "check",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--check"
    ],
    "required": false
  },
  {
    "default": false,
    "dest": "once",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--once"
    ],
    "required": false
  }
]
```

### `/external_contract/cli/mango-fish/result_contract`

<!-- exact-contract-value: 59b679c639824c9bf1023ba2405f98f889b7c68260ce8041aba95bec777a9ffd -->

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
      "id": "relay-pass-failed",
      "selected_by": {
        "kind": "relay-pass-reported-failures"
      },
      "stderr": {
        "all": "noncontractual-runtime-log"
      },
      "stdout": {
        "all": "no-command-result"
      }
    }
  ],
  "human_json_relationship": "mode-specific-results",
  "identity": "mango-fish-cli-result/root/v1",
  "profile_id": "mango-fish-cli-relay-runtime/v1",
  "structured_output": "mode-specific",
  "success": [
    {
      "exit_status": 0,
      "id": "configuration-check",
      "selected_by": {
        "kind": "option-equals",
        "parameter": "check",
        "value": true
      },
      "stderr": {
        "all": "noncontractual-runtime-log-or-empty"
      },
      "stdout": {
        "all": "mango-fish-configuration-summary/v1"
      }
    },
    {
      "exit_status": 0,
      "id": "relay-completed",
      "selected_by": {
        "kind": "option-equals",
        "parameter": "check",
        "value": false
      },
      "stderr": {
        "all": "noncontractual-runtime-log-or-empty"
      },
      "stdout": {
        "all": "no-command-result"
      }
    }
  ]
}
```

### `/external_contract/cli/mango-fish/subcommand_required`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/mango-fish/terminating_controls`

<!-- exact-contract-value: 565b13a16d5598879c9b05f935c428e916da3bfd7eb4eceff365b876b5bada56 -->

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
      "distribution": "mango-fish",
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
