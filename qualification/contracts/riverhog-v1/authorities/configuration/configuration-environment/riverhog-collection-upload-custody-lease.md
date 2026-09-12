# RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-collection-upload-custody-lease:2d14e7419c -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/configuration_environment/32`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configured-capacity/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE` — `configuration-environment:RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | configured-value | `operational_policy` | maximum=None, reason=operator-configured-capacity |

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 449cd7855ac1a721fa4bfcbff9f7ba902aa239ddfc68a8ed1ff06a371acd3fb0 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE"
}
```
