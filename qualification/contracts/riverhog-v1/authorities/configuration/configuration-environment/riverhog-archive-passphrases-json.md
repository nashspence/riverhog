# RIVERHOG_ARCHIVE_PASSPHRASES_JSON

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-archive-passphrases-json:d6ef6602fe -->

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
| `name` | "RIVERHOG_ARCHIVE_PASSPHRASES_JSON" |

## Governing policies

- `compatibility/configuration/v1`

## Evidence

### Qualification

- `make unit`
- `make compose-smoke`

### Executable sources

- `configuration-environment:RIVERHOG_ARCHIVE_PASSPHRASES_JSON` — `configuration-environment:RIVERHOG_ARCHIVE_PASSPHRASES_JSON`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/14`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f6dd2d723f7d9b8116caaacc139d068b041b5883ac4ec5841cf06fb16a87d853 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_ARCHIVE_PASSPHRASES_JSON"
}
```
