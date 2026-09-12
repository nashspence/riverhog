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

## Contract

```json
{
  "consumers": [
    "piggity"
  ],
  "name": "PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS"
}
```
