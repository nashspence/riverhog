# RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-archive-active-passphrase-id:1718e7ca5b -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/12`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID` — `configuration-environment:RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9476252509abf5f6bbc2eda8f2b4c22ffd5b7b91ba899374e0e916b84d08a75d -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID"
}
```
