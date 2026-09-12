# RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-retrieval-read-chunk-bytes:fa3c6d84c2 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/configuration_environment/71`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configured-capacity/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES` — `configuration-environment:RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES`
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
| `name` | "RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 266468e313c61f3c4e2c82113e5adace5aecac3930eddc9985f93dc23aa027b3 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES"
}
```
