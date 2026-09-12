# RIVERHOG_DATABASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-database-url:9c199f0a06 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-f77147b1c356"></a>
| Field | Shape |
|---|---|
| <a id="s-4504ac9a2419"></a>`consumers` | ["riverhog-server"] |
| <a id="s-8af67c498bf6"></a>`name` | "RIVERHOG_DATABASE_URL" |

## Governing policies

- <a id="pa-b61027f2e4a3"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:RIVERHOG_DATABASE_URL](../../../evidence/sources.md#src-e557a24df858) — `configuration-environment:RIVERHOG_DATABASE_URL`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/33`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b22760434c15f149a5fadb10d64a07de755bc99b05bf413b7318d1b50e3ad2dd -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_DATABASE_URL"
}
```
