# stove0

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0:stove0:cf6895ddfc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [cli](index.md) |
| Family | [root](families/root/index.md) |
| Contract elements | 1 |
| Extent decisions | 7 |

## External contract


### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-791a5ac62ab7"></a>`version` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --version |
| <a id="s-ddcd5174d2b6"></a>`base_url` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --base-url |
| <a id="s-7dc0e8294bca"></a>`token` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --token |
| <a id="s-f62c39ef757f"></a>`json_output` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |
| <a id="s-aae0a62f35f5"></a>`allow_insecure_http` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --allow-insecure-http |
| <a id="s-54084edd100e"></a>`install_completion` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --install-completion |
| <a id="s-5216637289dc"></a>`show_completion` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --show-completion |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --allow-insecure-http](#s-aae0a62f35f5) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --base-url](#s-ddcd5174d2b6) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --install-completion](#s-54084edd100e) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-f62c39ef757f) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --show-completion](#s-5216637289dc) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --token](#s-7dc0e8294bca) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --version](#s-791a5ac62ab7) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-d09e0f454f11"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-327fa397f61f"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d8812) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/name`
- `/external_contract/cli/stove0/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/name`

<!-- exact-contract-value: 12ae32cb1ec02d01eda3581b127c1fee3b0dc53572ed6baf239721a03d82e126 -->

```json
""
```

### `/external_contract/cli/stove0/parameters`

<!-- exact-contract-value: 27817a8b5af625ab0a68de36b2bb2e1429d160e66a7ebcca783657718cd8b547 -->

```json
[
  {
    "count": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "version",
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
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "base_url",
    "nargs": 1,
    "options": [
      "--base-url"
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
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "token",
    "nargs": 1,
    "options": [
      "--token"
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
    "name": "json_output",
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
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "allow_insecure_http",
    "nargs": 1,
    "options": [
      "--allow-insecure-http"
    ],
    "required": false,
    "secondary_options": [
      "--no-allow-insecure-http"
    ],
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
