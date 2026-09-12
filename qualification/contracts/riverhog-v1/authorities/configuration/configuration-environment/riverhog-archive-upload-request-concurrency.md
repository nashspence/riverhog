# RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-archive-upload-request-concurrency:c572ae009b -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/configuration_environment/19`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configured-capacity/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY` — `configuration-environment:RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY`
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
| `name` | "RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 77bb88aabe208ff97dbef7a2a4fa6dbc45ebec0766a68bf285d0a2cf1000867f -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY"
}
```
