# RIVERHOG_PROVENANCE_STATE_HOME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-provenance-state-home:cdf1d02b3d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `consumers` | ["riverhog-provenance"] |
| `name` | "RIVERHOG_PROVENANCE_STATE_HOME" |

## Governing policies

- `compatibility/configuration/v1`

## Evidence

### Qualification

- `make unit`
- `make compose-smoke`

### Executable sources

- `configuration-environment:RIVERHOG_PROVENANCE_STATE_HOME` — `configuration-environment:RIVERHOG_PROVENANCE_STATE_HOME`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/55`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 34353d7c2d47daaacfa034b08c1b2a693b0b397a3352533bf4e136d4f8ba48b3 -->

```json
{
  "consumers": [
    "riverhog-provenance"
  ],
  "name": "RIVERHOG_PROVENANCE_STATE_HOME"
}
```
