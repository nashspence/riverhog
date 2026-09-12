# RIVERHOG_DOWNLOAD_FILE_CONCURRENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-download-file-concurrency:b5ea9e29e3 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/configuration_environment/34`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configured-capacity/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_DOWNLOAD_FILE_CONCURRENCY` — `configuration-environment:RIVERHOG_DOWNLOAD_FILE_CONCURRENCY`
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
| `name` | "RIVERHOG_DOWNLOAD_FILE_CONCURRENCY" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d0d67e64f48b91e32bc36bdd176b7a77838be2aff10ec078d2bf156096859a70 -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "name": "RIVERHOG_DOWNLOAD_FILE_CONCURRENCY"
}
```
