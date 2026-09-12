# RIVERHOG_BROWSE_TOKEN_SIGNING_KEY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-browse-token-signing-key:4ac221bf7c -->

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
| `name` | "RIVERHOG_BROWSE_TOKEN_SIGNING_KEY" |

## Governing policies

- `compatibility/configuration/v1`

## Evidence

### Qualification

- `make unit`
- `make compose-smoke`

### Executable sources

- `configuration-environment:RIVERHOG_BROWSE_TOKEN_SIGNING_KEY` — `configuration-environment:RIVERHOG_BROWSE_TOKEN_SIGNING_KEY`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/26`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2b76e30812a08c0e06ef60d9162e2a9df9d9afacfe07ee179b2dac0dbd348f7d -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_BROWSE_TOKEN_SIGNING_KEY"
}
```
