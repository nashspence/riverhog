# PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:piggity-upload-finalize-poll-seconds:2b47f46a96 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/configuration_environment/7`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configured-capacity/v1`

## Executable sources and proof

- `configuration-environment:PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS` — `configuration-environment:PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS`
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
| `name` | "PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e8fe9cc66b3ddf76f9a71f4d2f1af74152b2d746b6188cdd977d31202fd1f86b -->

```json
{
  "consumers": [
    "piggity"
  ],
  "name": "PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS"
}
```
