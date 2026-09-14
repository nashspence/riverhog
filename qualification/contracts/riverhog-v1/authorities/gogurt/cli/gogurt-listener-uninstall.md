# gogurt listener uninstall

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-listener-uninstall:8ee46ed425 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-bf49e85607"></a>Parser name: `uninstall`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-9bf6e52d46"></a>`listener_host_provider` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --listener-host-provider |
| <a id="s-9f84e8d6b1"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Result and failure contract

- <a id="s-7309472bd3"></a>Result identity: `gogurt-cli-result/listener/uninstall/v1`
- <a id="s-3845455ef3"></a>Profile: `gogurt-cli-human-json/v1`
- <a id="s-4aad861e0c"></a>Structured output: `optional-json`
- <a id="s-a1c234178f"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-3d05495670"></a>`completed` | <a id="s-b5bb63891f"></a>`0` | <a id="s-f498ce6822"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-2752e77382"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-a145130e57"></a>`usage` | <a id="s-1efb38269b"></a>`2` | <a id="s-c32269de3c"></a>`{"all":"empty"}` | <a id="s-17bfb09a33"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-af71b8dcf5"></a>`operational` | <a id="s-33bfc36d90"></a>`1` | <a id="s-5f08400698"></a>`{"human":"empty","json":"gogurt-cli-error/v1"}` | <a id="s-2428b3eefe"></a>`{"human":"noncontractual-diagnostic","json":"empty"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-9f84e8d6b1) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --listener-host-provider](#s-9bf6e52d46) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-47ebdf327c"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-baedb65e65"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources.md#src-3b2297c37d) — `reference/gogurt/application/src/gogurt/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/gogurt/commands/listener/commands/uninstall/name`
- `/external_contract/cli/gogurt/commands/listener/commands/uninstall/parameters`
- `/external_contract/cli/gogurt/commands/listener/commands/uninstall/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/listener/commands/uninstall/name`

<!-- exact-contract-value: 40fdac7cdc15fc93453dfc8a21ab0ff042c64c52dfaac794da0e9f625d9357a0 -->

```json
"uninstall"
```

### `/external_contract/cli/gogurt/commands/listener/commands/uninstall/parameters`

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

### `/external_contract/cli/gogurt/commands/listener/commands/uninstall/result_contract`

<!-- exact-contract-value: 990398fc04b1dbd6e149cdb09313f20ce36abb28fbd0973a00001a39977a4da1 -->

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
  "identity": "gogurt-cli-result/listener/uninstall/v1",
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
