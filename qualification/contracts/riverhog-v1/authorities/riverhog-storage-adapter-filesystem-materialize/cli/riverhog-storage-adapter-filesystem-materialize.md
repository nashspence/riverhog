# riverhog-storage-adapter-filesystem-materialize

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-storage-adapter-filesy-89bc075d61:riverhog-storage-adapter-filesystem-materialize:511ca05446 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-filesystem-materialize` |
| Interface | `cli` |
| Family | `root` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/cli/riverhog-storage-adapter-filesystem-materialize/name`
- `/external_contract/cli/riverhog-storage-adapter-filesystem-materialize/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:riverhog-storage-adapter-filesystem-materialize` — `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/materialize_cli.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=0, minimum=0, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=0, minimum=0, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=0, minimum=0, reason=fixed-command-argument-arity |

## Contract

- Parser name: `riverhog-storage-adapter-filesystem-materialize`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `` | _VersionAction | no |  | --version |
| `` | _StoreAction | yes | Path |  |
| `` | _StoreAction | yes | Path |  |
| `` | _AppendAction | no |  | --path |
| `` | _AppendAction | no |  | --prefix |
| `` | _StoreTrueAction | no |  | --all |
| `` | _StoreTrueAction | no |  | --json |
