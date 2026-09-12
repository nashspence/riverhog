# RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-event-context-reap-batch-size:d85fcf5925 -->

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
| `consumers` | ["riverhog-server"] |
| `name` | "RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE" |

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

- `configuration-environment:RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE` — `configuration-environment:RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/37`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: afd313c075766908be097411390f7e51b843be812fb38dbb6372f13164a15e97 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE"
}
```
