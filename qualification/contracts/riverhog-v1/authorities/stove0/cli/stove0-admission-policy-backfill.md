# stove0 admission policy backfill

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0:stove0-admission-policy-backfill:4d829d3f4f -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `cli` |
| Family | `admission` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/cli/stove0/commands/admission/commands/policy/commands/backfill/name`
- `/external_contract/cli/stove0/commands/admission/commands/policy/commands/backfill/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:stove0` — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Related interface records

- [Operation parity: backfill_admission_policy](../operation/operation-parity-backfill-admission-policy.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |

## Contract

- Parser name: `backfill`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `policy_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | policy_id |
