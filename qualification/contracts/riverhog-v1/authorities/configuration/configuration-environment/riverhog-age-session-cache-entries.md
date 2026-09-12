# RIVERHOG_AGE_SESSION_CACHE_ENTRIES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-age-session-cache-entries:1c49e75159 -->

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
| `name` | "RIVERHOG_AGE_SESSION_CACHE_ENTRIES" |

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

- `configuration-environment:RIVERHOG_AGE_SESSION_CACHE_ENTRIES` — `configuration-environment:RIVERHOG_AGE_SESSION_CACHE_ENTRIES`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/9`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e801d838f15105871e8ca8b826e081eabe6b4da71cc28ee0facbf50d62f3a9a4 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_AGE_SESSION_CACHE_ENTRIES"
}
```
