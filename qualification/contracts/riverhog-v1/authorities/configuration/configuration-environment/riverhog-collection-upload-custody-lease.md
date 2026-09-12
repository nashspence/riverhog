# RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-collection-upload-custody-lease:2d14e7419c -->

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
| `name` | "RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE" |

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

- `configuration-environment:RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE` — `configuration-environment:RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/32`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 449cd7855ac1a721fa4bfcbff9f7ba902aa239ddfc68a8ed1ff06a371acd3fb0 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE"
}
```
