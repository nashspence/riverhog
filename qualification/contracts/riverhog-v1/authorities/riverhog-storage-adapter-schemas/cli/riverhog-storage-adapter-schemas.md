# riverhog-storage-adapter-schemas

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-storage-adapter-schemas:riverhog-storage-adapter-schemas:70bfaac081 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-schemas` |
| Interface | `cli` |
| Family | `root` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/cli/riverhog-storage-adapter-schemas/name`
- `/external_contract/cli/riverhog-storage-adapter-schemas/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:riverhog-storage-adapter-schemas` — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=0, minimum=0, reason=fixed-command-argument-arity |

## Contract

- Parser name: `riverhog-storage-adapter-schemas`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `` | _StoreAction | no | Path | --output |
| `` | _StoreTrueAction | no |  | --compact |
