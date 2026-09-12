# RIVERHOG_ALLOW_INSECURE_HTTP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-allow-insecure-http:2e5af2ee82 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/11`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_ALLOW_INSECURE_HTTP` — `configuration-environment:RIVERHOG_ALLOW_INSECURE_HTTP`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (3 items) |
| `name` | "RIVERHOG_ALLOW_INSECURE_HTTP" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7763338d7157144f83ae2912f603e8c04f8aa2105bfdf4dcd23eff2a37ae5e76 -->

```json
{
  "consumers": [
    "riverhog-client",
    "riverhog-ftp-adapter",
    "stove0-server"
  ],
  "name": "RIVERHOG_ALLOW_INSECURE_HTTP"
}
```
