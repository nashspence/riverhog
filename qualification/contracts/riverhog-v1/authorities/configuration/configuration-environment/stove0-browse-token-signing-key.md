# STOVE0_BROWSE_TOKEN_SIGNING_KEY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-browse-token-signing-key:cbbb4d1111 -->

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
| `consumers` | ["stove0-server"] |
| `name` | "STOVE0_BROWSE_TOKEN_SIGNING_KEY" |

## Governing policies

- `compatibility/configuration/v1`

## Evidence

### Qualification

- `make unit`
- `make compose-smoke`

### Executable sources

- `configuration-environment:STOVE0_BROWSE_TOKEN_SIGNING_KEY` — `configuration-environment:STOVE0_BROWSE_TOKEN_SIGNING_KEY`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/83`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 21ede63a620e23233fefa4f6acf5b4b7d3813b8be0a6ca8aa8a7d86f5fcd9cef -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_BROWSE_TOKEN_SIGNING_KEY"
}
```
