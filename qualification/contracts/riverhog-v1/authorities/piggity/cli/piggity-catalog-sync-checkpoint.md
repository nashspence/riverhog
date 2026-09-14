# piggity catalog-sync checkpoint

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-catalog-sync-checkpoint:81be6036e3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-d3fa47b142"></a>Parser name: `checkpoint`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-dcf2b49b77"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Result and failure contract

- <a id="s-2b0b2b2d1f"></a>Result identity: `piggity-cli-result/catalog-sync/checkpoint/v1`
- <a id="s-6862e73e69"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-caf119ba46"></a>Structured output: `optional-json`
- <a id="s-6cb892b83a"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-0b2d7883da"></a>`completed` | <a id="s-252be26c00"></a>`0` | <a id="s-ebe7483078"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-47c63667db"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-7681051a14"></a>`usage` | <a id="s-21afa4537a"></a>`2` | <a id="s-13641a298c"></a>`{"all":"empty"}` | <a id="s-e1af3e495e"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-a88d115143"></a>`operational` | <a id="s-9d05105219"></a>`1` | <a id="s-82f0f7edf1"></a>`{"human":"empty","json":"http-api-contracts.ErrorResponse"}` | <a id="s-afe412afa3"></a>`{"human":"noncontractual-diagnostic","json":"empty"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-dcf2b49b77) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/catalog-sync/checkpoint](../../riverhog/http-operations/get-v1-catalog-sync-checkpoint.md)

## Governing policies

- <a id="pa-88008bab1f"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-394010159a"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/catalog-sync/commands/checkpoint/name`
- `/external_contract/cli/piggity/commands/catalog-sync/commands/checkpoint/parameters`
- `/external_contract/cli/piggity/commands/catalog-sync/commands/checkpoint/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/catalog-sync/commands/checkpoint/name`

<!-- exact-contract-value: 59b774a5253e2bc8e357bdfc95e4f7a4b214734f6da65a9f53b40711a10ba16b -->

```json
"checkpoint"
```

### `/external_contract/cli/piggity/commands/catalog-sync/commands/checkpoint/parameters`

<!-- exact-contract-value: f2cf9ed04ac608b58219dbcf22fc63be2fdf35901bc058f443df21b229aefd32 -->

```json
[
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

### `/external_contract/cli/piggity/commands/catalog-sync/commands/checkpoint/result_contract`

<!-- exact-contract-value: 9d69120d246c1668a3e62ecc735e935e5a4b9117eee7539a7e916e9130a6056b -->

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
  "identity": "piggity-cli-result/catalog-sync/checkpoint/v1",
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
