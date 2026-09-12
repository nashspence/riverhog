# STOVE0_ALLOW_INSECURE_HTTP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-allow-insecure-http:ddda4f16d0 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/79`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:STOVE0_ALLOW_INSECURE_HTTP` — `configuration-environment:STOVE0_ALLOW_INSECURE_HTTP`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "STOVE0_ALLOW_INSECURE_HTTP" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 840ec66382e0b1e00ee135e9bcfc95c207a44355a6498f9dc4398eae6af5e771 -->

```json
{
  "consumers": [
    "stove0-api-client"
  ],
  "name": "STOVE0_ALLOW_INSECURE_HTTP"
}
```
