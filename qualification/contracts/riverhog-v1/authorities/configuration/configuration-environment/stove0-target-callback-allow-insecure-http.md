# STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-target-callback-allow-insecure-http:863ce92729 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/113`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP` — `configuration-environment:STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 048d152730400dbab3affefd03b86e4bc657e632918812a95132a5fff10e49b5 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP"
}
```
