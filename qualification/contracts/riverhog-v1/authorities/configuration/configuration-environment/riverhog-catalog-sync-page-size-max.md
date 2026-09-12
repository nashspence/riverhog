# RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-catalog-sync-page-size-max:0c41edb266 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/configuration_environment/31`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configured-capacity/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX` — `configuration-environment:RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX`
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
| `name` | "RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d18ef7b22fb469f9692f9471ab509eb4ea74100c0323a14de60bd9697c017672 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX"
}
```
