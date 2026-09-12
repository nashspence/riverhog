# RIVERHOG_FTP_ADAPTER_CONFIG

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-ftp-adapter-config:343b1f81d0 -->

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
| `consumers` | ["riverhog-ftp-adapter"] |
| `name` | "RIVERHOG_FTP_ADAPTER_CONFIG" |

## Governing policies

- `compatibility/configuration/v1`

## Evidence

### Qualification

- `make unit`
- `make compose-smoke`

### Executable sources

- `configuration-environment:RIVERHOG_FTP_ADAPTER_CONFIG` — `configuration-environment:RIVERHOG_FTP_ADAPTER_CONFIG`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/42`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: abe11f8b9b5115d2defebd9ae29cdeee8aa928191d7f781a5052f6d7957e04ea -->

```json
{
  "consumers": [
    "riverhog-ftp-adapter"
  ],
  "name": "RIVERHOG_FTP_ADAPTER_CONFIG"
}
```
