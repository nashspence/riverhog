# RIVERHOG_BROWSE_TOKEN_SIGNING_KEY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-browse-token-signing-key:4ac221bf7c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-0ef9db2534"></a>
| Field | Shape |
|---|---|
| <a id="s-3d50fc447d"></a>`consumers` | ["riverhog-server"] |
| <a id="s-8eed53d4d3"></a>`name` | "RIVERHOG_BROWSE_TOKEN_SIGNING_KEY" |

## Governing policies

- <a id="pa-1b93bd5f0d"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_BROWSE_TOKEN_SIGNING_KEY](../../../evidence/sources.md#src-4e8fa6daa7) — `configuration-environment:RIVERHOG_BROWSE_TOKEN_SIGNING_KEY`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

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
