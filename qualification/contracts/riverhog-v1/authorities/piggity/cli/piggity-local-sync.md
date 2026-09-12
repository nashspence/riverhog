# piggity local sync

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-local-sync:e57cc3d544 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [cli](index.md) |
| Family | [local](families/local/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

- <a id="s-d3e6816ced"></a>Parser name: `sync`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-6368e8bf41"></a>`wait` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --wait |
| <a id="s-78b95fe0d7"></a>`restore_policy` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --restore-policy |
| <a id="s-b7ec88d2f5"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-b7ec88d2f5) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --restore-policy](#s-78b95fe0d7) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --wait](#s-6368e8bf41) | `cardinality · values-per-occurrence · fixed` | shared above |

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

- <a id="pa-d9ec8c06fe"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-d0ac48e20d"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/local/commands/sync/name`
- `/external_contract/cli/piggity/commands/local/commands/sync/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/local/commands/sync/name`

<!-- exact-contract-value: 5cfa1a73f845669922af31371796703a709a5489a6db9091aaf64d8d905aedde -->

```json
"sync"
```

### `/external_contract/cli/piggity/commands/local/commands/sync/parameters`

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
