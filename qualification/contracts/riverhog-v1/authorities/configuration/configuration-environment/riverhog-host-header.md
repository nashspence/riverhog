# RIVERHOG_HOST_HEADER

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-host-header:63f66cafee -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-c5d59cd44b"></a>
| Field | Shape |
|---|---|
| <a id="s-4676feab29"></a>`consumers` | ["riverhog-client"] |
| <a id="s-0774de54c0"></a>`name` | "RIVERHOG_HOST_HEADER" |

## Governing policies

- <a id="pa-633b63c3ae"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_HOST_HEADER](../../../evidence/sources.md#src-d40a0dd079) — `configuration-environment:RIVERHOG_HOST_HEADER`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/46`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6078ec8517653d7cb85b3c43a317c75a5b35c33a8c974eeefa73cc03424d9e1f -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "name": "RIVERHOG_HOST_HEADER"
}
```
