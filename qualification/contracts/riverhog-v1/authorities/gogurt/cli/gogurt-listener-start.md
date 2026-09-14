# gogurt listener start

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-listener-start:e5567a120d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-b1de582a6c"></a>Parser name: `start`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-803a3ef3e9"></a>`listener_host_provider` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --listener-host-provider |
| <a id="s-ee7d3bdc45"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Result and failure contract

- <a id="s-de0497bbc6"></a>Result identity: `gogurt-cli-result/listener/start/v1`
- <a id="s-67e71b44ed"></a>Profile: `gogurt-cli-human-json/v1`
- <a id="s-fe7e19d4f9"></a>Structured output: `optional-json`
- <a id="s-a512eb3ec4"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-6500d7576d"></a>`completed` | <a id="s-9055574a5b"></a>`0` | <a id="s-ce27145fb6"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-9fec4eff17"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-61af2ef932"></a>`usage` | <a id="s-0b83952898"></a>`2` | <a id="s-2d1cddc1e0"></a>`{"all":"empty"}` | <a id="s-951938b362"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-652373259d"></a>`operational` | <a id="s-055e1b2724"></a>`1` | <a id="s-2e3213e14e"></a>`{"human":"empty","json":"gogurt-cli-error/v1"}` | <a id="s-49ba0ac894"></a>`{"human":"noncontractual-diagnostic","json":"empty"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-ee7d3bdc45) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --listener-host-provider](#s-803a3ef3e9) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-92bfb1b1ad"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-093719de9b"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources.md#src-3b2297c37d) — `reference/gogurt/application/src/gogurt/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/gogurt/commands/listener/commands/start/name`
- `/external_contract/cli/gogurt/commands/listener/commands/start/parameters`
- `/external_contract/cli/gogurt/commands/listener/commands/start/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/listener/commands/start/name`

<!-- exact-contract-value: a92ae9615600f7f0bcb0edf9703b379c163bef33ed749ae40c48a0830d4ab6ae -->

```json
"start"
```

### `/external_contract/cli/gogurt/commands/listener/commands/start/parameters`

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

### `/external_contract/cli/gogurt/commands/listener/commands/start/result_contract`

<!-- exact-contract-value: cbdee21374a9ebdb556bb436da3b8311ff90e703856b3214e2fb909d1ccde936 -->

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
  "identity": "gogurt-cli-result/listener/start/v1",
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
