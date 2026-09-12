# RIVERHOG_HOST_HEADER

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-host-header:63f66cafee -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/46`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_HOST_HEADER` — `configuration-environment:RIVERHOG_HOST_HEADER`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "RIVERHOG_HOST_HEADER" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6078ec8517653d7cb85b3c43a317c75a5b35c33a8c974eeefa73cc03424d9e1f -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "name": "RIVERHOG_HOST_HEADER"
}
```
