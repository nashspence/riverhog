# RIVERHOG_ARCHIVE_WRITE_STORE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-archive-write-store:7b36be187c -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/22`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_ARCHIVE_WRITE_STORE` — `configuration-environment:RIVERHOG_ARCHIVE_WRITE_STORE`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "RIVERHOG_ARCHIVE_WRITE_STORE" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b68d7f8ae5f695d170f32fb6584bc6ba712cba7fc84929f4f721d8108bdb6b36 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_ARCHIVE_WRITE_STORE"
}
```
