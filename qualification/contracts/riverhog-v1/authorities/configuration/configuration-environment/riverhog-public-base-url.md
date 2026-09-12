# RIVERHOG_PUBLIC_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-public-base-url:0ed641ed6d -->

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
| `consumers` | ["riverhog-server"] |
| `name` | "RIVERHOG_PUBLIC_BASE_URL" |

## Governing policies

- `compatibility/configuration/v1`

## Evidence

### Qualification

- `make unit`
- `make compose-smoke`

### Executable sources

- `configuration-environment:RIVERHOG_PUBLIC_BASE_URL` — `configuration-environment:RIVERHOG_PUBLIC_BASE_URL`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/56`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1416d7a583c2ee256c77e25eadf1c655bb5a3c695c2b01442c0b923278a1275d -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_PUBLIC_BASE_URL"
}
```
