# RIVERHOG_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-token:447bb29ff0 -->

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
| `consumers` | ["riverhog-client","stove0-server"] |
| `name` | "RIVERHOG_TOKEN" |

## Governing policies

- `compatibility/configuration/v1`

## Evidence

### Qualification

- `make unit`
- `make compose-smoke`

### Executable sources

- `configuration-environment:RIVERHOG_TOKEN` — `configuration-environment:RIVERHOG_TOKEN`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/74`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 02bb5d873171d334db5807231f1467b059aa4ec05c0954bb5ba58bb52ff5f48c -->

```json
{
  "consumers": [
    "riverhog-client",
    "stove0-server"
  ],
  "name": "RIVERHOG_TOKEN"
}
```
