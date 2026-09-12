# piggity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity:c2de8fb83b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [cli](index.md) |
| Family | [root](families/root/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract


### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-1179f261dfd4"></a>`_version` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --version |
| <a id="s-e9fe18e2e0dc"></a>`install_completion` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --install-completion |
| <a id="s-b13f8792e0af"></a>`show_completion` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --show-completion |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --version](#s-1179f261dfd4) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --install-completion](#s-e9fe18e2e0dc) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --show-completion](#s-b13f8792e0af) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-8185b7512ccb"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-b347d97be5de"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f2c) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/name`
- `/external_contract/cli/piggity/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/name`

<!-- exact-contract-value: 12ae32cb1ec02d01eda3581b127c1fee3b0dc53572ed6baf239721a03d82e126 -->

```json
""
```

### `/external_contract/cli/piggity/parameters`

<!-- exact-contract-value: 6c3eaa1e80ed7eec26f0605768d3652ccdb37504c04b8b85a9dc9a439b170363 -->

```json
[
  {
    "count": false,
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "_version",
    "nargs": 1,
    "options": [
      "--version"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "install_completion",
    "nargs": 1,
    "options": [
      "--install-completion"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "show_completion",
    "nargs": 1,
    "options": [
      "--show-completion"
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
