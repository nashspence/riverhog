# PIGGITY_UPLOAD_FILE_LOG_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:piggity-upload-file-log-bytes:fb82247d97 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/configuration_environment/6`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configured-capacity/v1`

## Executable sources and proof

- `configuration-environment:PIGGITY_UPLOAD_FILE_LOG_BYTES` — `configuration-environment:PIGGITY_UPLOAD_FILE_LOG_BYTES`
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
| `name` | "PIGGITY_UPLOAD_FILE_LOG_BYTES" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 841d2006ff78de844f649893a93c5c9a712f68554db85b586643064f5ac99ce1 -->

```json
{
  "consumers": [
    "piggity"
  ],
  "name": "PIGGITY_UPLOAD_FILE_LOG_BYTES"
}
```
