# piggity collection provenance verification-cancel

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-provenance-verification-cancel:ffb4beca81 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-9e7a276178"></a>Parser name: `verification-cancel`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-d966bbd28c"></a>`collection_id` | TyperArgument | yes | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | collection_id |
| <a id="s-7eca69dae1"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Result and failure contract

- <a id="s-cdbc079ae3"></a>Result identity: `piggity-cli-result/collection/provenance/verification-cancel/v1`
- <a id="s-c835bce029"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-92cfd0bc9b"></a>Structured output: `optional-json`
- <a id="s-2db0b4423e"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-3d834cdbaf"></a>`completed` | <a id="s-26490bee49"></a>`0` | <a id="s-66bba8dad1"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-435e7d6c31"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-8c680bed41"></a>`usage` | <a id="s-7adde907fb"></a>`2` | <a id="s-90fa0441be"></a>`{"all":"empty"}` | <a id="s-99f3c60d65"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-ec45fedc50"></a>`operational` | <a id="s-36a90f06fc"></a>`1` | <a id="s-80cf024386"></a>`{"human":"empty","json":"http-api-contracts.ErrorResponse"}` | <a id="s-ae17552a75"></a>`{"human":"noncontractual-diagnostic","json":"empty"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-d966bbd28c) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-7eca69dae1) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [DELETE /v1/collections/{collection_id}/provenance/verification](../../riverhog/http-operations/delete-v1-collections-collection-id-provenance-verification.md)

## Governing policies

- <a id="pa-ab0c212aa5"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-c65d75d5c3"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-cancel/name`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-cancel/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-cancel/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-cancel/name`

<!-- exact-contract-value: 910bb6e80e3b26665adde72400f34b7925959fd6853bfbaa1e64f8cc2e34be5f -->

```json
"verification-cancel"
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-cancel/parameters`

<!-- exact-contract-value: 69121b7dd4df39852c314f302ca34fb358e3d4472f9e5565bdec50242e30ee3a -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "collection_id",
    "nargs": 1,
    "options": [
      "collection_id"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.IntParamType",
      "name": "integer"
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

### `/external_contract/cli/piggity/commands/collection/commands/provenance/commands/verification-cancel/result_contract`

<!-- exact-contract-value: aa1d7af241f74d13effedcb30c71074a7594217ec81b5e8c264bec27f58894d8 -->

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
  "identity": "piggity-cli-result/collection/provenance/verification-cancel/v1",
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
