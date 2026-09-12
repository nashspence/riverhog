# RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-ftp-adapter-http-timeout-seconds:dd2944347f -->

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
| `consumers` | ["riverhog-ftp-adapter-api-client"] |
| `name` | "RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS" |

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

- `configuration-environment:RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS` — `configuration-environment:RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/44`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fb922dba02e71fa21438f668b84f633d312b79a59d69bd0d9cd4a118ad0c06d0 -->

```json
{
  "consumers": [
    "riverhog-ftp-adapter-api-client"
  ],
  "name": "RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS"
}
```
