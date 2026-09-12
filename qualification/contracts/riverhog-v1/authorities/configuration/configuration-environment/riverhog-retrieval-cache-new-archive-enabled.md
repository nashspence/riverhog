# RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-retrieval-cache-new-archive-enabled:294aa7a18a -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/58`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED` — `configuration-environment:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b4fc8f31da80f5567b7335844b9ab31582e97ec8118012881b2d62480a69d48a -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED"
}
```
