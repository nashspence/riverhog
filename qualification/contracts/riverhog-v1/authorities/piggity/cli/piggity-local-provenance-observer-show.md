# piggity local provenance-observer show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-local-provenance-observer-show:96a8bd80c8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-1ba9a6a3ee"></a>Parser name: `show`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-6d86b9c512"></a>`name` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | name |
| <a id="s-85ac36453b"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Result and failure contract

- <a id="s-899a9a9ce3"></a>Result identity: `piggity-cli-result/local/provenance-observer/show/v1`
- <a id="s-686fd1e75a"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-f353e2b8f0"></a>Structured output: `optional-json`
- <a id="s-a85cd600e0"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-88f645520e"></a>`completed` | <a id="s-c62e270b9a"></a>`0` | <a id="s-8d9961d10f"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-1bde3632ac"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-ebd0fe5612"></a>`usage` | <a id="s-faeed2bedf"></a>`2` | <a id="s-c6abe5a8ff"></a>`{"all":"empty"}` | <a id="s-4514ee0392"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-e7dae9deaf"></a>`operational` | <a id="s-eebe915706"></a>`1` | <a id="s-e7371c6eda"></a>`{"human":"empty","json":"http-api-contracts.ErrorResponse"}` | <a id="s-ad67867972"></a>`{"human":"noncontractual-diagnostic","json":"empty"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-85ac36453b) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter name](#s-6d86b9c512) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-7c812c6e8d"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-0f6952c052"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/show/name`
- `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/show/parameters`
- `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/show/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/show/parameters`

<!-- exact-contract-value: 5fc7dfae38d65070ca7b7f9d59934174307d6a3212e66d1af82fe71c64370451 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "name",
    "nargs": 1,
    "options": [
      "name"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  },
  {
    "count": false,
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "json_mode",
    "nargs": 1,
    "options": [
      "--json"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
    }
  }
]
```

### `/external_contract/cli/piggity/commands/local/commands/provenance-observer/commands/show/result_contract`

<!-- exact-contract-value: 70e0921bf116dc60cbc49b5764321abac8a559b9abad808c7b8cd906d4b048fd -->

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
      "id": "operational",
      "stderr": {
        "human": "noncontractual-diagnostic",
        "json": "empty"
      },
      "stdout": {
        "human": "empty",
        "json": "http-api-contracts.ErrorResponse"
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "piggity-cli-result/local/provenance-observer/show/v1",
  "profile_id": "piggity-cli-human-json/v1",
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
