# mango-fish state verify

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:mango-fish:mango-fish-state-verify:55015d62bf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [mango-fish](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-9527b6167f"></a>Parser name: `verify`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-7505345121"></a>`json` | _StoreTrueAction | no |  | --json |

### Result and failure contract

- <a id="s-6327dd9720"></a>Result identity: `mango-fish-cli-result/state/verify/v1`
- <a id="s-4b384d33d3"></a>Profile: `mango-fish-cli-state-human-json/v1`
- <a id="s-4994b906c5"></a>Structured output: `optional-json`
- <a id="s-f90c4c742b"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-cf072c036d"></a>`completed` | <a id="s-71b34aab03"></a>`0` | <a id="s-114723fad1"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-fd16c3d55b"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-cfb5d4af33"></a>`usage` | <a id="s-081ff352f1"></a>`2` | <a id="s-0845615c84"></a>`{"all":"empty"}` | <a id="s-9d889bd126"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-103859e7e6"></a>`state-schema` | <a id="s-f74b41fde2"></a>`1` | <a id="s-d87cb320b7"></a>`{"all":"empty"}` | <a id="s-51dbf4baca"></a>`{"all":"mango-fish-state-schema-diagnostic/v1"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-7505345121) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-86ed36e4d1"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-95f50b4803"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:mango-fish](../../../evidence/sources.md#src-3dcd5eedf2) — `reference/riverhog/applications/mango-fish/src/mango_fish/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/mango-fish/commands/state/commands/verify/name`
- `/external_contract/cli/mango-fish/commands/state/commands/verify/parameters`
- `/external_contract/cli/mango-fish/commands/state/commands/verify/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/mango-fish/commands/state/commands/verify/name`

<!-- exact-contract-value: 898c74c2eed0452b1e51e567f237c37f1caa1e52f747466d56e76e15d07dc331 -->

```json
"verify"
```

### `/external_contract/cli/mango-fish/commands/state/commands/verify/parameters`

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

### `/external_contract/cli/mango-fish/commands/state/commands/verify/result_contract`

<!-- exact-contract-value: a84094ed6da52bf638bc2a757eb1506c2d081b18ab3f7f80943ee8db5ee4cc6b -->

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
  "identity": "mango-fish-cli-result/state/verify/v1",
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
