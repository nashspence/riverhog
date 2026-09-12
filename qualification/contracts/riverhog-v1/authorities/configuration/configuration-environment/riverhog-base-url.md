# RIVERHOG_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-base-url:8b47959f9f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b211a29ced"></a>
| Field | Shape |
|---|---|
| <a id="s-9c278af545"></a>`consumers` | ["riverhog-client","riverhog-ftp-adapter","stove0-server"] |
| <a id="s-18bbe5ebd0"></a>`name` | "RIVERHOG_BASE_URL" |

## Governing policies

- <a id="pa-1bb3fdadf3"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_BASE_URL](../../../evidence/sources.md#src-0396e6ba9f) — `configuration-environment:RIVERHOG_BASE_URL`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/23`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c0b4a8d74a96d90cb2dbf063668b3499bca2d3d8cb2bb0c06eed250e01706021 -->

```json
{
  "consumers": [
    "riverhog-client",
    "riverhog-ftp-adapter",
    "stove0-server"
  ],
  "name": "RIVERHOG_BASE_URL"
}
```
