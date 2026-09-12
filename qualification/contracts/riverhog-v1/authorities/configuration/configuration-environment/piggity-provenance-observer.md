# PIGGITY_PROVENANCE_OBSERVER

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:piggity-provenance-observer:7274288831 -->

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
| `consumers` | ["piggity"] |
| `name` | "PIGGITY_PROVENANCE_OBSERVER" |

## Governing policies

- `compatibility/configuration/v1`

## Evidence

### Qualification

- `make unit`
- `make compose-smoke`

### Executable sources

- `configuration-environment:PIGGITY_PROVENANCE_OBSERVER` — `configuration-environment:PIGGITY_PROVENANCE_OBSERVER`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/5`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 42759cf7f177f82d580e465305a24c36bafb9858a9b37b0780ae910274299329 -->

```json
{
  "consumers": [
    "piggity"
  ],
  "name": "PIGGITY_PROVENANCE_OBSERVER"
}
```
