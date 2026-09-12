# RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-retrieval-request-concurrency:e1d2fe610d -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/configuration_environment/72`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configured-capacity/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY` — `configuration-environment:RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY`
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
| `name` | "RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f716c7798dcb781ac44afb906405ecb7f361e4406cca8c3ce3872b6d81a0d723 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY"
}
```
