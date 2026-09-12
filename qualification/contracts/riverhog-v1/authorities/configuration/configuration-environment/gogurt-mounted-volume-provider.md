# GOGURT_MOUNTED_VOLUME_PROVIDER

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:gogurt-mounted-volume-provider:706cf99e7d -->

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
| `consumers` | ["gogurt"] |
| `name` | "GOGURT_MOUNTED_VOLUME_PROVIDER" |

## Governing policies

- `compatibility/configuration/v1`

## Evidence

### Qualification

- `make unit`
- `make compose-smoke`

### Executable sources

- `configuration-environment:GOGURT_MOUNTED_VOLUME_PROVIDER` — `configuration-environment:GOGURT_MOUNTED_VOLUME_PROVIDER`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/1`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 09fd0c5a895533f8f9d7bba28538d2f69f9e685dd837e93dcbc59a861da66b34 -->

```json
{
  "consumers": [
    "gogurt"
  ],
  "name": "GOGURT_MOUNTED_VOLUME_PROVIDER"
}
```
