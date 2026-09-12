# gogurt-routes configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:gogurt-routes:gogurt-routes-configuration:24f4f1c1be -->

| Audit field | Value |
|---|---|
| Authority | `gogurt-routes` |
| Interface | `configuration` |
| Family | `documents` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/configuration_documents/gogurt-routes`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configuration-composition/v1`

## Executable sources and proof

- `configuration:gogurt-routes` — `reference/gogurt/packages/core/src/gogurt_core/__init__.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |
| cardinality | entries | `operational_policy` | maximum=None, reason=validated-deployment-composition |

## Contract

- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `kind` | yes | string |  |
| `routes` | yes | object |  |
| `schema_version` | yes | integer |  |
