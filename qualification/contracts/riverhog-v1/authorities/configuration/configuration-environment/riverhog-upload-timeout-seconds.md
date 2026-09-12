# RIVERHOG_UPLOAD_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-upload-timeout-seconds:390aa7ba63 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/configuration_environment/77`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configured-capacity/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_UPLOAD_TIMEOUT_SECONDS` — `configuration-environment:RIVERHOG_UPLOAD_TIMEOUT_SECONDS`
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
| `name` | "RIVERHOG_UPLOAD_TIMEOUT_SECONDS" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9f61ccdb9b3b414711281f34e686a61dcda01ae6af40bcf20387e1e616454592 -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "name": "RIVERHOG_UPLOAD_TIMEOUT_SECONDS"
}
```
