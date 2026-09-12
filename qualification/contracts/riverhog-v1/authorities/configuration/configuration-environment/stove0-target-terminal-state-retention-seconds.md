# STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-target-terminal-state-retention-seconds:e13cf9bd3c -->

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
| `consumers` | ["stove0-target-support"] |
| `name` | "STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS" |

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

- `configuration-environment:STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS` — `configuration-environment:STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/116`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c1476afe5be8b5a91c3e6cc400580431a0b534ff8a2e996fbfa0185a61a01bbf -->

```json
{
  "consumers": [
    "stove0-target-support"
  ],
  "name": "STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS"
}
```
