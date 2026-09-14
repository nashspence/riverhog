# gogurt mounts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-mounts:f7df8c5f6c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-350ba3c065"></a>Parser name: `mounts`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-df906ece38"></a>`mounted_volume_provider` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --mounted-volume-provider |
| <a id="s-498ab6f222"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Result and failure contract

- <a id="s-78b46c977e"></a>Result identity: `gogurt-cli-result/mounts/v1`
- <a id="s-45929e7d85"></a>Profile: `gogurt-cli-human-json/v1`
- <a id="s-9fb02ae803"></a>Structured output: `optional-json`
- <a id="s-eca49855d8"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-00a3f402c0"></a>`completed` | <a id="s-5890ba2bbd"></a>`0` | <a id="s-c57f9c0623"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-133e8dfafd"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-c4c1416683"></a>`usage` | <a id="s-22a8a7f9c0"></a>`2` | <a id="s-b934d81bab"></a>`{"all":"empty"}` | <a id="s-50d61a0459"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-39d0526896"></a>`operational` | <a id="s-dd58a8cb2b"></a>`1` | <a id="s-0f8b9a0013"></a>`{"human":"empty","json":"gogurt-cli-error/v1"}` | <a id="s-2f5f6ba1dc"></a>`{"human":"noncontractual-diagnostic","json":"empty"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-498ab6f222) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --mounted-volume-provider](#s-df906ece38) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-81216d7afe"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-a5f0127d50"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources.md#src-3b2297c37d) — `reference/gogurt/application/src/gogurt/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/gogurt/commands/mounts/name`
- `/external_contract/cli/gogurt/commands/mounts/parameters`
- `/external_contract/cli/gogurt/commands/mounts/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/mounts/name`

<!-- exact-contract-value: 09a1a78edad0483f330a615c7189ec42febdd6dee239f4f50c1c1809a5cb52a3 -->

```json
"mounts"
```

### `/external_contract/cli/gogurt/commands/mounts/parameters`

<!-- exact-contract-value: 83c1089fc312d926e38bc2c1895b5a74fe68544af45e9bfb4d7c6279a94e8215 -->

```json
[
  {
    "count": false,
    "envvar": "GOGURT_MOUNTED_VOLUME_PROVIDER",
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "mounted_volume_provider",
    "nargs": 1,
    "options": [
      "--mounted-volume-provider"
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

### `/external_contract/cli/gogurt/commands/mounts/result_contract`

<!-- exact-contract-value: 03ec04c19dfab4b3e78d1c149a2e19605af8029b80a2b0e342676b022a109e91 -->

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
  "identity": "gogurt-cli-result/mounts/v1",
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
