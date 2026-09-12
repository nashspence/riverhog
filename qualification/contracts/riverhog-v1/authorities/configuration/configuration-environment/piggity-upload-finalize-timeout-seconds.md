# PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:piggity-upload-finalize-timeout-seconds:bc0bfe2b8d -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/configuration_environment/8`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configured-capacity/v1`

## Executable sources and proof

- `configuration-environment:PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS` — `configuration-environment:PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS`
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
| `name` | "PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7194cf68695a92fb6a2862f7cd79d769e84a7d42d58c0e03561e1da113d5beae -->

```json
{
  "consumers": [
    "piggity"
  ],
  "name": "PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS"
}
```
