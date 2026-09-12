# piggity local repair

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-local-repair:0b1434d45e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [cli](index.md) |
| Family | [local](families/local/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

- <a id="s-9e8311971e"></a>Parser name: `repair`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-3f96fb6ed3"></a>`wait` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --wait |
| <a id="s-0caf5df218"></a>`restore_policy` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --restore-policy |
| <a id="s-6cd35e75ee"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-6cd35e75ee) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --restore-policy](#s-0caf5df218) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --wait](#s-3f96fb6ed3) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [Operation parity: get_portable_collection_inventory](../../riverhog/operation/operation-parity-get-portable-collection-inventory.md)
- [Operation parity: get_collection](../../riverhog/operation/operation-parity-get-collection.md)
- [Operation parity: list_collection_tags](../../riverhog/operation/operation-parity-list-collection-tags.md)
- [Operation parity: create_retrieval_job](../../riverhog/operation/operation-parity-create-retrieval-job.md)
- [Operation parity: get_retrieval_job](../../riverhog/operation/operation-parity-get-retrieval-job.md)
- [Operation parity: acknowledge_retrieval_job](../../riverhog/operation/operation-parity-acknowledge-retrieval-job.md)
- [Operation parity: download_retrieval_file](../../riverhog/operation/operation-parity-download-retrieval-file.md)
- [Operation parity: renew_retrieval_job](../../riverhog/operation/operation-parity-renew-retrieval-job.md)
- [Operation parity: plan_retrieval](../../riverhog/operation/operation-parity-plan-retrieval.md)
- [Operation parity: advance_retrieval_plan](../../riverhog/operation/operation-parity-advance-retrieval-plan.md)
- [Operation parity: list_retrieval_plan_files](../../riverhog/operation/operation-parity-list-retrieval-plan-files.md)

## Governing policies

- <a id="pa-9d2b136bb4"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-4bef3e76d9"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/local/commands/repair/name`
- `/external_contract/cli/piggity/commands/local/commands/repair/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/local/commands/repair/name`

<!-- exact-contract-value: 15b490ee176bfe3f1065942fd75feec8e02c2d9beeaca063b55a973f0da8c0e4 -->

```json
"repair"
```

### `/external_contract/cli/piggity/commands/local/commands/repair/parameters`

<!-- exact-contract-value: 6d6f63e6cd222ae7a2b7a7bcbac898d6478aced320f5378d56f9d313ff59ebd7 -->

```json
[
  {
    "count": false,
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "wait",
    "nargs": 1,
    "options": [
      "--wait"
    ],
    "required": false,
    "secondary_options": [
      "--no-wait"
    ],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
    }
  },
  {
    "count": false,
    "default": "allow",
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "restore_policy",
    "nargs": 1,
    "options": [
      "--restore-policy"
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
