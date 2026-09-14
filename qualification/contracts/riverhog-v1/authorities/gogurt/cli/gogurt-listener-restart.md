# gogurt listener restart

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-listener-restart:e58be115ee -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-ac32d59250"></a>Parser name: `restart`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-dbe5643cac"></a>`listener_host_provider` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --listener-host-provider |
| <a id="s-da26c387d5"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Result and failure contract

- <a id="s-635cf9b58d"></a>Result identity: `gogurt-cli-result/listener/restart/v1`
- <a id="s-45c0f415a7"></a>Profile: `gogurt-cli-human-json/v1`
- <a id="s-f7148aeecd"></a>Structured output: `optional-json`
- <a id="s-a63f64cb84"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-2c0740f018"></a>`completed` | <a id="s-26bfa21678"></a>`0` | <a id="s-d25d2e8000"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-f05a5c9f17"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-3e30a69b6d"></a>`usage` | <a id="s-f0aa8522e9"></a>`2` | <a id="s-8f434a665f"></a>`{"all":"empty"}` | <a id="s-c29092f5ee"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-e7943d4ce9"></a>`operational` | <a id="s-ed44622329"></a>`1` | <a id="s-d38046a23f"></a>`{"human":"empty","json":"gogurt-cli-error/v1"}` | <a id="s-b8a39e506e"></a>`{"human":"noncontractual-diagnostic","json":"empty"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-da26c387d5) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --listener-host-provider](#s-dbe5643cac) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-68ebed37a5"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-a892155961"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources.md#src-3b2297c37d) — `reference/gogurt/application/src/gogurt/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/gogurt/commands/listener/commands/restart/name`
- `/external_contract/cli/gogurt/commands/listener/commands/restart/parameters`
- `/external_contract/cli/gogurt/commands/listener/commands/restart/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/listener/commands/restart/name`

<!-- exact-contract-value: 84841d3a0e2418b75dec3d5df55954803d61a26a4cd821d8df25908486b04a55 -->

```json
"restart"
```

### `/external_contract/cli/gogurt/commands/listener/commands/restart/parameters`

<!-- exact-contract-value: fe7b56d2906ac79e76d31abc11e3bbc7c7ed0b66ee89c57133a60c8c1242d2fb -->

```json
[
  {
    "count": false,
    "envvar": "GOGURT_LISTENER_HOST_PROVIDER",
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "listener_host_provider",
    "nargs": 1,
    "options": [
      "--listener-host-provider"
    ],
    "required": false,
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

### `/external_contract/cli/gogurt/commands/listener/commands/restart/result_contract`

<!-- exact-contract-value: 80a691b1ae7ff6e0875d7d66f5a3262bc5ea1bc286de5043826ea85e005794d0 -->

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
        "json": "gogurt-cli-error/v1"
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "gogurt-cli-result/listener/restart/v1",
  "profile_id": "gogurt-cli-human-json/v1",
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
