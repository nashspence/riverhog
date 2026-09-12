# riverhog-storage-adapter-conformance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-storage-adapter-conformance:riverhog-storage-adapter-conformance:6cf179f1ba -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-conformance` |
| Interface | `cli` |
| Family | `root` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/cli/riverhog-storage-adapter-conformance/name`
- `/external_contract/cli/riverhog-storage-adapter-conformance/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:riverhog-storage-adapter-conformance` — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/conformance.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=0, minimum=0, reason=fixed-command-argument-arity |

## Contract

- Parser name: `riverhog-storage-adapter-conformance`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `` | _StoreAction | yes |  | --base-url |
| `` | _StoreAction | yes | Path | --token-file |
| `` | _StoreAction | yes |  | --object-prefix |
| `` | _StoreTrueAction | no |  | --allow-insecure-http |
