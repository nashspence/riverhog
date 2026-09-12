# RIVERHOG_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-base-url:8b47959f9f -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/23`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_BASE_URL` — `configuration-environment:RIVERHOG_BASE_URL`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (3 items) |
| `name` | "RIVERHOG_BASE_URL" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c0b4a8d74a96d90cb2dbf063668b3499bca2d3d8cb2bb0c06eed250e01706021 -->

```json
{
  "consumers": [
    "riverhog-client",
    "riverhog-ftp-adapter",
    "stove0-server"
  ],
  "name": "RIVERHOG_BASE_URL"
}
```
