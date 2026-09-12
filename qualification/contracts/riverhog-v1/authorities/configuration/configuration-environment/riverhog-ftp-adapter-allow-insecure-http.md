# RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-ftp-adapter-allow-insecure-http:0f02bf74a5 -->

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
| `consumers` | ["riverhog-ftp-adapter-api-client"] |
| `name` | "RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP" |

## Governing policies

- `compatibility/configuration/v1`

## Evidence

### Qualification

- `make unit`
- `make compose-smoke`

### Executable sources

- `configuration-environment:RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP` — `configuration-environment:RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/40`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fbc27c92dfb581e67fb969bb3f829f36479487be2343d8974725c0a3b0000706 -->

```json
{
  "consumers": [
    "riverhog-ftp-adapter-api-client"
  ],
  "name": "RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP"
}
```
