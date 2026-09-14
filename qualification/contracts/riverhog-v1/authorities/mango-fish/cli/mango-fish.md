# mango-fish

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:mango-fish:mango-fish:0da0ebace4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [mango-fish](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-8e837eac2f"></a>Parser name: `mango-fish`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-1368f192cf"></a>`version` | _VersionAction | no |  | --version |
| <a id="s-b3bbac485d"></a>`config` | _StoreAction | yes | Path | --config |
| <a id="s-55e6dc25a4"></a>`check` | _StoreTrueAction | no |  | --check |
| <a id="s-04b810bfdf"></a>`once` | _StoreTrueAction | no |  | --once |

### Result and failure contract

- <a id="s-604d4c4bd9"></a>Result identity: `mango-fish-cli-result/root/v1`
- <a id="s-64173d57e0"></a>Profile: `mango-fish-cli-relay-runtime/v1`
- <a id="s-d637f6e309"></a>Structured output: `mode-specific`
- <a id="s-dae04cf130"></a>Human/JSON relationship: `mode-specific-results`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-c5c84fd8e9"></a>`configuration-check` | <a id="s-a0dd530aa6"></a>`0` | <a id="s-1c5b2334bf"></a>`{"all":"mango-fish-configuration-summary/v1"}` | <a id="s-0d4de0c37a"></a>`{"all":"noncontractual-runtime-log-or-empty"}` |
| <a id="s-e4784b90bb"></a>`relay-completed` | <a id="s-22cd70bc63"></a>`0` | <a id="s-09536f2b2c"></a>`{"all":"no-command-result"}` | <a id="s-9bfa84e578"></a>`{"all":"noncontractual-runtime-log-or-empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-cdbaee25f6"></a>`usage` | <a id="s-c4a4f0a60b"></a>`2` | <a id="s-beb6fde10b"></a>`{"all":"empty"}` | <a id="s-dcf9ebe4b8"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-e215ef5c5f"></a>`relay-pass-failed` | <a id="s-aef55108b0"></a>`1` | <a id="s-33ac94278e"></a>`{"all":"no-command-result"}` | <a id="s-0469ca77fd"></a>`{"all":"noncontractual-runtime-log"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --version](#s-1368f192cf) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --check](#s-55e6dc25a4) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --once](#s-04b810bfdf) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-519bad0cca"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-d96d013113"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:mango-fish](../../../evidence/sources.md#src-3dcd5eedf2) — `reference/riverhog/applications/mango-fish/src/mango_fish/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/mango-fish/name`
- `/external_contract/cli/mango-fish/parameters`
- `/external_contract/cli/mango-fish/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/mango-fish/name`

<!-- exact-contract-value: 1597bf55bb7889ac7479259c9f4631adf1e2188403282e8d82efa49b1815e160 -->

```json
"mango-fish"
```

### `/external_contract/cli/mango-fish/parameters`

<!-- exact-contract-value: 86e7f99d8c26c4a2af74645430cc84a84863561bbcf8a62fc165a551c7e65335 -->

```json
[
  {
    "dest": "version",
    "kind": "_VersionAction",
    "nargs": 0,
    "options": [
      "--version"
    ],
    "required": false
  },
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

<!-- exact-contract-value: 966de8f840f449c632ed1270c472367463e238a55e1ea936a8a8e5bb2aaf5035 -->

```json
{
  "failures": [
    {
      "exit_status": 2,
      "id": "usage",
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
