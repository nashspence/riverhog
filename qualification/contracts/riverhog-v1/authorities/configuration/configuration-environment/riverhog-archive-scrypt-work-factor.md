# RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-archive-scrypt-work-factor:91ad8b229a -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/17`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR` — `configuration-environment:RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8e21a62470c22ba1612d73051278a28e7945f77071967090a05648c307fe143a -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR"
}
```
