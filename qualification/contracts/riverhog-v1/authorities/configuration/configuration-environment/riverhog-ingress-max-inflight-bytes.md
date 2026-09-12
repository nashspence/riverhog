# RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-ingress-max-inflight-bytes:2b3d75f38d -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/configuration_environment/49`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configured-capacity/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES` — `configuration-environment:RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES`
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
| `name` | "RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 571aee796161d24ff211ebd579669c4a7b667be882d2a0767cdce0fd4571a7b8 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES"
}
```
