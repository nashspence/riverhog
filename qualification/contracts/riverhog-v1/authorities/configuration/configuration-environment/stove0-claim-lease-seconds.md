# STOVE0_CLAIM_LEASE_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-claim-lease-seconds:654731b407 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

| Field | Shape |
|---|---|
| `consumers` | ["stove0-server"] |
| `name` | "STOVE0_CLAIM_LEASE_SECONDS" |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| value | configured-value | `operational_policy` | maximum=None, reason=operator-configured-capacity |

## Governing policies

- `compatibility/configuration/v1`
- `extent-rule/configured-capacity/v1`

## Evidence

### Qualification

- `make unit`
- `make compose-smoke`

### Executable sources

- `configuration-environment:STOVE0_CLAIM_LEASE_SECONDS` — `configuration-environment:STOVE0_CLAIM_LEASE_SECONDS`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/85`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0977e9c8ff7b9981ace64b4e1d9cac4c93d4e61761343326e75834c1f8fd9eca -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_CLAIM_LEASE_SECONDS"
}
```
