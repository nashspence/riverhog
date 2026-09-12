# gogurt listener _run

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-listener-run:fc612b6a35 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [cli](index.md) |
| Family | [listener](index.md#f-75084d8061df) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- <a id="s-fb1d923bfc32"></a>Parser name: `_run`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-0c097bf54de6"></a>`runtime_config` | TyperOption | yes | {'class': 'typer.models.TyperPath', 'name': 'path'} | --runtime-config |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --runtime-config](#s-0c097bf54de6) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-5102a50df2ca"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-0d8ae0ff485e"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [cli:gogurt](../../../evidence/sources.md#src-3b2297c37dfe) — `reference/gogurt/application/src/gogurt/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/gogurt/commands/listener/commands/_run/name`
- `/external_contract/cli/gogurt/commands/listener/commands/_run/parameters`

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
