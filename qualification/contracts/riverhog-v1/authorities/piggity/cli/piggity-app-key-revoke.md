# piggity app key revoke

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-app-key-revoke:813a82b47e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-9c780f52a5"></a>Parser name: `revoke`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-6c6eb4f988"></a>`app_name` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | app_name |
| <a id="s-15b43c36fc"></a>`key_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | key_id |
| <a id="s-96ecefa133"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Result and failure contract

- <a id="s-76df4c7cb3"></a>Result identity: `piggity-cli-result/app/key/revoke/v1`
- <a id="s-d9c9798dbf"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-8e944e6385"></a>Structured output: `optional-json`
- <a id="s-8104562953"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-5287729f12"></a>`completed` | <a id="s-369a035560"></a>`0` | <a id="s-049336a602"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-ff4b62f10e"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-eacc493597"></a>`usage` | <a id="s-a3f90aae8f"></a>`2` | <a id="s-a8bfda6ceb"></a>`{"all":"empty"}` | <a id="s-bd7ddce154"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-c096dffa60"></a>`operational` | <a id="s-6181f7aa27"></a>`1` | <a id="s-2fa2065ffa"></a>`{"human":"empty","json":"http-api-contracts.ErrorResponse"}` | <a id="s-bdc4f8c32f"></a>`{"human":"noncontractual-diagnostic","json":"empty"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter app_name](#s-6c6eb4f988) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-96ecefa133) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter key_id](#s-15b43c36fc) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [POST /v1/apps/{app}/keys/{key_id}/revoke](../../riverhog/http-operations/post-v1-apps-app-keys-key-id-revoke.md)

## Governing policies

- <a id="pa-2617552499"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-b4f332b385"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/app/commands/key/commands/revoke/name`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/revoke/parameters`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/revoke/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/app/commands/key/commands/revoke/name`

<!-- exact-contract-value: 4dbb5115705a0de2a6127443c67e751a0baa58326bad39f890b1efc796cc6a3a -->

```json
"revoke"
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/revoke/parameters`

<!-- exact-contract-value: 1749f130449a17b41843bc114da325f40e7d6ade60854043985e5bcec11d2cad -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "app_name",
    "nargs": 1,
    "options": [
      "app_name"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  },
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "key_id",
    "nargs": 1,
    "options": [
      "key_id"
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

### `/external_contract/cli/piggity/commands/app/commands/key/commands/revoke/result_contract`

<!-- exact-contract-value: 4c56584cca16cd76bac3b20fecacecd35f9a0cd26349cdbedf70684b865024ca -->

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
  "identity": "piggity-cli-result/app/key/revoke/v1",
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
