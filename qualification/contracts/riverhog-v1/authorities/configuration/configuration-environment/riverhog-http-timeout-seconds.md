# RIVERHOG_HTTP_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-http-timeout-seconds:d0a1512599 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/configuration_environment/48`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configured-capacity/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_HTTP_TIMEOUT_SECONDS` — `configuration-environment:RIVERHOG_HTTP_TIMEOUT_SECONDS`
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
| `name` | "RIVERHOG_HTTP_TIMEOUT_SECONDS" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f6283c7fc7becef59b0b0aa64a9a94ddcaff6f7ec6deacba82cda6295d05221f -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "name": "RIVERHOG_HTTP_TIMEOUT_SECONDS"
}
```
