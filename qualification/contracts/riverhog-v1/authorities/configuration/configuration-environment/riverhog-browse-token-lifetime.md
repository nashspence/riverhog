# RIVERHOG_BROWSE_TOKEN_LIFETIME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-browse-token-lifetime:dfe5cfe8fc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1b7215e25153"></a>
| Field | Shape |
|---|---|
| <a id="s-84c48e52004d"></a>`consumers` | ["riverhog-server"] |
| <a id="s-399d989cc4ae"></a>`name` | "RIVERHOG_BROWSE_TOKEN_LIFETIME" |

## Governing policies

- <a id="pa-2b4719391236"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:RIVERHOG_BROWSE_TOKEN_LIFETIME](../../../evidence/sources.md#src-fe8e77928379) — `configuration-environment:RIVERHOG_BROWSE_TOKEN_LIFETIME`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/25`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0fb099cd29921ac366335faa171608f4ca428320fd8b99c6178be930def6f8d3 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_BROWSE_TOKEN_LIFETIME"
}
```
