# RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-ingress-source-read-chunk-bytes:51b1621e81 -->

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
| `name` | "RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES" |

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

- `configuration-environment:RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES` — `configuration-environment:RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/50`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c21bb8504f997adbdfad629a4f8bc4b89a332afde126b83e673e9fdac7872d48 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES"
}
```
