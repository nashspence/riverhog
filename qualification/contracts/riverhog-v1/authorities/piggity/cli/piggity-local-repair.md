# piggity local repair

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-local-repair:0b1434d45e -->

| Audit field | Value |
|---|---|
| Authority | `piggity` |
| Interface | `cli` |
| Family | `local` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/cli/piggity/commands/local/commands/repair/name`
- `/external_contract/cli/piggity/commands/local/commands/repair/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:piggity` — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Related interface records

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

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |

## Contract summary

- Parser name: `repair`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `wait` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --wait |
| `restore_policy` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --restore-policy |
| `json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

## Complete owned contract

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
