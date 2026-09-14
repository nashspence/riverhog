# piggity retrieval cache status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-retrieval-cache-status:c78a8551a0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-cd2252804c"></a>Parser name: `status`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-fffd71beac"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Result and failure contract

- <a id="s-e7560570e4"></a>Result identity: `piggity-cli-result/retrieval/cache/status/v1`
- <a id="s-574f281e70"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-b0c71591d2"></a>Structured output: `optional-json`
- <a id="s-e3b803a9fe"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-13d2cdccbe"></a>`completed` | <a id="s-1754f2a616"></a>`0` | <a id="s-65d6b0257b"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-a34a7a1724"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-486ce53318"></a>`usage` | <a id="s-3db902f788"></a>`2` | <a id="s-6b588973f2"></a>`{"all":"empty"}` | <a id="s-842f344a57"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-8cb7a7e021"></a>`operational` | <a id="s-5db1d65837"></a>`1` | <a id="s-9b115e39c8"></a>`{"human":"empty","json":"http-api-contracts.ErrorResponse"}` | <a id="s-0d6acdc611"></a>`{"human":"noncontractual-diagnostic","json":"empty"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-fffd71beac) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/retrieval-cache](../../riverhog/http-operations/get-v1-retrieval-cache.md)

## Governing policies

- <a id="pa-e97d0525de"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-1fb18a4911"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/status/name`
- `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/status/parameters`
- `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/status/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/status/name`

<!-- exact-contract-value: cfc31bcc34ed7f4cc7895026ae8a54f0494f73757e9f914d0f6ed90f9bc34f51 -->

```json
"status"
```

### `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/status/parameters`

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

### `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/status/result_contract`

<!-- exact-contract-value: 513c0cd31b20fe311bedc5ec549fed1c34d0e0bfb495e1ed8e801ae6f12b55a7 -->

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
  "identity": "piggity-cli-result/retrieval/cache/status/v1",
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
