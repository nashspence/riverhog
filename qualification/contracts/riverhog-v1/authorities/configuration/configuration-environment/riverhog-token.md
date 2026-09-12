# RIVERHOG_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-token:447bb29ff0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-e17f28d8d86b"></a>
| Field | Shape |
|---|---|
| <a id="s-a8f98984dcec"></a>`consumers` | ["riverhog-client","stove0-server"] |
| <a id="s-72292b572547"></a>`name` | "RIVERHOG_TOKEN" |

## Governing policies

- <a id="pa-562da040e525"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:RIVERHOG_TOKEN](../../../evidence/sources.md#src-30337f36d13d) — `configuration-environment:RIVERHOG_TOKEN`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/74`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 02bb5d873171d334db5807231f1467b059aa4ec05c0954bb5ba58bb52ff5f48c -->

```json
{
  "consumers": [
    "riverhog-client",
    "stove0-server"
  ],
  "name": "RIVERHOG_TOKEN"
}
```
