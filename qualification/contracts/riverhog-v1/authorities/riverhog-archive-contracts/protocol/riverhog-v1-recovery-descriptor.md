# Riverhog v1 recovery descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-archive-contracts:riverhog-v1-recovery-descriptor:29e07b6fb5 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-archive-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1schemas~1riverhog-recovery-descriptor-v1.schema.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-recovery-descriptor-v1.schema.json` — `packages/riverhog-archive-contracts/schemas/riverhog-recovery-descriptor-v1.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract

- `$id`: https://nashspence.github.io/riverhog/v1/schemas/riverhog-recovery-descriptor-v1.schema.json
- `title`: Riverhog v1 recovery descriptor
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `encryption` | yes | object |  |
| `root` | yes | object |  |
| `schema` | yes | object (1 fields) |  |
