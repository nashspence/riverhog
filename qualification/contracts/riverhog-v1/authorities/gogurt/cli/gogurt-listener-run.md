# gogurt listener _run

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-listener-run:4a2850bbfa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-fb1d923bfc"></a>Parser name: `_run`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-0c097bf54d"></a>`runtime_config` | TyperOption | yes | {'class': 'typer.models.TyperPath', 'name': 'path'} | --runtime-config |

### Result and failure contract

- <a id="s-0953e99ed7"></a>Result identity: `gogurt-cli-result/listener/_run/v1`
- <a id="s-100697cec4"></a>Profile: `gogurt-cli-listener-runtime/v1`
- <a id="s-b13021d8a5"></a>Structured output: `none`
- <a id="s-a8b4591f12"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-b8d539a5aa"></a>`stopped` | <a id="s-826785cebb"></a>`0` | <a id="s-45f82f8b45"></a>`{"human":"no-command-result"}` | <a id="s-d9fc2fe84c"></a>`{"human":"noncontractual-runtime-status"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-70bf80372a"></a>`usage` | <a id="s-6d66a7e4f8"></a>`2` | <a id="s-8baffb6201"></a>`{"human":"empty"}` | <a id="s-3937824054"></a>`{"human":"noncontractual-usage-diagnostic"}` |
| <a id="s-f9b0a7cbde"></a>`operational` | <a id="s-dbce6f4e51"></a>`1` | <a id="s-e4a64afda9"></a>`{"human":"empty"}` | <a id="s-d26e9b641e"></a>`{"human":"noncontractual-diagnostic"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --runtime-config](#s-0c097bf54d) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-57e0324e73"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-89774b74f1"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources.md#src-3b2297c37d) — `reference/gogurt/application/src/gogurt/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/gogurt/commands/listener/commands/_run/name`
- `/external_contract/cli/gogurt/commands/listener/commands/_run/parameters`
- `/external_contract/cli/gogurt/commands/listener/commands/_run/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/listener/commands/_run/name`

<!-- exact-contract-value: 5f0b1cc0fff7e256dbce362a64c00555715bcd2eb6cd0040718cee353633a7ff -->

```json
"_run"
```

### `/external_contract/cli/gogurt/commands/listener/commands/_run/parameters`

<!-- exact-contract-value: bcc4ae3bf32fb126fe4759e5f09ca3b2a76c909da7f41cd96b017dac9a244080 -->

```json
[
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "runtime_config",
    "nargs": 1,
    "options": [
      "--runtime-config"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer.models.TyperPath",
      "name": "path"
    }
  }
]
```

### `/external_contract/cli/gogurt/commands/listener/commands/_run/result_contract`

<!-- exact-contract-value: 66331dff6fdecfe69b16a890b1f274e02c594f73f5ab005aaef122f0f3e61ac4 -->

```json
{
  "failures": [
    {
      "exit_status": 2,
      "id": "usage",
      "stderr": {
        "human": "noncontractual-usage-diagnostic"
      },
      "stdout": {
        "human": "empty"
      }
    },
    {
      "exit_status": 1,
      "id": "operational",
      "stderr": {
        "human": "noncontractual-diagnostic"
      },
      "stdout": {
        "human": "empty"
      }
    }
  ],
  "human_json_relationship": "not-applicable",
  "identity": "gogurt-cli-result/listener/_run/v1",
  "profile_id": "gogurt-cli-listener-runtime/v1",
  "structured_output": "none",
  "success": [
    {
      "exit_status": 0,
      "id": "stopped",
      "stderr": {
        "human": "noncontractual-runtime-status"
      },
      "stdout": {
        "human": "no-command-result"
      }
    }
  ]
}
```
