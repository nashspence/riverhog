# PIGGITY_LOCAL_ROOT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:piggity-local-root:2bf9880123 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/3`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:PIGGITY_LOCAL_ROOT` — `configuration-environment:PIGGITY_LOCAL_ROOT`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "PIGGITY_LOCAL_ROOT" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 55b6221cb389005b4efa5aaf31cf3649cdb4a2b0afd8cf5b98554e72dc443175 -->

```json
{
  "consumers": [
    "piggity"
  ],
  "name": "PIGGITY_LOCAL_ROOT"
}
```
