# mango-fish state status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:mango-fish:mango-fish-state-status:4acfec2a1a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [mango-fish](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-23c16a2129"></a>Parser name: `status`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-f1aea638f4"></a>`json` | _StoreTrueAction | no |  | --json |

### Result and failure contract

- <a id="s-a7ffa71a3e"></a>Result identity: `mango-fish-cli-result/state/status/v1`
- <a id="s-8163a11e67"></a>Profile: `mango-fish-cli-state-human-json/v1`
- <a id="s-fef5b3d8f3"></a>Structured output: `optional-json`
- <a id="s-09784fc8e9"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-1c9d261761"></a>`completed` | <a id="s-3576ac6723"></a>`0` | <a id="s-a41133c2a5"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-47792e58f1"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-83d4e893ff"></a>`usage` | <a id="s-0ed756ac5e"></a>`2` | <a id="s-bde8a0e7e0"></a>`{"all":"empty"}` | <a id="s-ce98abc8ed"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-261943abb1"></a>`state-schema` | <a id="s-ff1a92103f"></a>`1` | <a id="s-f09a63bb89"></a>`{"all":"empty"}` | <a id="s-afd2ae98a4"></a>`{"all":"mango-fish-state-schema-diagnostic/v1"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-f1aea638f4) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-00efcc02dd"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-d4713fd46c"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:mango-fish](../../../evidence/sources.md#src-3dcd5eedf2) — `reference/riverhog/applications/mango-fish/src/mango_fish/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/mango-fish/commands/state/commands/status/name`
- `/external_contract/cli/mango-fish/commands/state/commands/status/parameters`
- `/external_contract/cli/mango-fish/commands/state/commands/status/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/mango-fish/commands/state/commands/status/name`

<!-- exact-contract-value: cfc31bcc34ed7f4cc7895026ae8a54f0494f73757e9f914d0f6ed90f9bc34f51 -->

```json
"status"
```

### `/external_contract/cli/mango-fish/commands/state/commands/status/parameters`

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

### `/external_contract/cli/mango-fish/commands/state/commands/status/result_contract`

<!-- exact-contract-value: 46a5c4f5f0c95232444051900df5e7af7d23441e381a3ea89dcca3fd22f2001f -->

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
  "identity": "mango-fish-cli-result/state/status/v1",
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
