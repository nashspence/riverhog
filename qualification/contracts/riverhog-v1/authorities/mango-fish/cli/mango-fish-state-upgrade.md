# mango-fish state upgrade

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:mango-fish:mango-fish-state-upgrade:7c1630d4b4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [mango-fish](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-1b645cf3be"></a>Parser name: `upgrade`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-0d0b0accba"></a>`json` | _StoreTrueAction | no |  | --json |

### Result and failure contract

- <a id="s-1f9c641006"></a>Result identity: `mango-fish-cli-result/state/upgrade/v1`
- <a id="s-6634f70e7f"></a>Profile: `mango-fish-cli-state-human-json/v1`
- <a id="s-4ec1d75573"></a>Structured output: `optional-json`
- <a id="s-7fc1728ff8"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-99b8ba6e3a"></a>`completed` | <a id="s-1695390eac"></a>`0` | <a id="s-f9d3635779"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-6b236ec5eb"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-205080976d"></a>`usage` | <a id="s-9607183810"></a>`2` | <a id="s-3b217d607e"></a>`{"all":"empty"}` | <a id="s-74b01bcee7"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-f375f7e14a"></a>`state-schema` | <a id="s-58d1bc86e3"></a>`1` | <a id="s-b86235213d"></a>`{"all":"empty"}` | <a id="s-9c8df75786"></a>`{"all":"mango-fish-state-schema-diagnostic/v1"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-0d0b0accba) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-8bf7ce2bc9"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-405623be06"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:mango-fish](../../../evidence/sources.md#src-3dcd5eedf2) — `reference/riverhog/applications/mango-fish/src/mango_fish/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/mango-fish/commands/state/commands/upgrade/name`
- `/external_contract/cli/mango-fish/commands/state/commands/upgrade/parameters`
- `/external_contract/cli/mango-fish/commands/state/commands/upgrade/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/mango-fish/commands/state/commands/upgrade/name`

<!-- exact-contract-value: 192e80d1fe4e27d140b2db67853ff131d7a670e024c440098f759d2df9f2c230 -->

```json
"upgrade"
```

### `/external_contract/cli/mango-fish/commands/state/commands/upgrade/parameters`

<!-- exact-contract-value: a4eee56605df36f2261ef19e35bdc3c16631d89a610d870dece837b2b2866441 -->

```json
[
  {
    "default": false,
    "dest": "json",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--json"
    ],
    "required": false
  }
]
```

### `/external_contract/cli/mango-fish/commands/state/commands/upgrade/result_contract`

<!-- exact-contract-value: 87468d09191c3c8590aba7247771a414690daddd927b6d1cd96e4ea946c55233 -->

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
      "id": "state-schema",
      "stderr": {
        "all": "mango-fish-state-schema-diagnostic/v1"
      },
      "stdout": {
        "all": "empty"
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "mango-fish-cli-result/state/upgrade/v1",
  "profile_id": "mango-fish-cli-state-human-json/v1",
  "structured_output": "optional-json",
  "success": [
    {
      "exit_status": 0,
      "id": "completed",
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": "named-command-result"
      }
    }
  ]
}
```
