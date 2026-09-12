# mango-fish configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:mango-fish:mango-fish-configuration:cf7172ea24 -->

| Audit field | Value |
|---|---|
| Authority | `mango-fish` |
| Interface | `configuration` |
| Family | `documents` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/configuration_documents/mango-fish`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configuration-composition/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `configuration:mango-fish` — `reference/riverhog/applications/mango-fish/src/mango_fish/relay.py::MangoFishConfig`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |

## Contract

- `title`: MangoFishConfig
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `batch_size` | no | integer |  |
| `poll_interval_seconds` | no | number |  |
| `request_timeout_seconds` | no | number |  |
| `sources` | yes | array |  |
| `state_path` | yes | string |  |
| `version` | no | integer |  |

### Definitions

| Definition | Shape |
|---|---|
| `SourceConfig` | object |
