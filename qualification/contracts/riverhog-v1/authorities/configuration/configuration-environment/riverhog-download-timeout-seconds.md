# RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-download-timeout-seconds:5868d6be91 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/configuration_environment/36`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configured-capacity/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS` — `configuration-environment:RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS`
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
| `name` | "RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8ad4f976d6c4931ba2cb4ee7ac1c7d653aeba56c71bbf9792aed72c023781774 -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "name": "RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS"
}
```
