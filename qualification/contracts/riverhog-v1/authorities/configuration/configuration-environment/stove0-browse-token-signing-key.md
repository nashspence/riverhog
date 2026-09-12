# STOVE0_BROWSE_TOKEN_SIGNING_KEY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-browse-token-signing-key:cbbb4d1111 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-658650862c"></a>
| Field | Shape |
|---|---|
| <a id="s-e8619b82d7"></a>`consumers` | ["stove0-server"] |
| <a id="s-dc1bc511db"></a>`name` | "STOVE0_BROWSE_TOKEN_SIGNING_KEY" |

## Governing policies

- <a id="pa-d1d99a890a"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:STOVE0_BROWSE_TOKEN_SIGNING_KEY](../../../evidence/sources.md#src-b93955cc12) — `configuration-environment:STOVE0_BROWSE_TOKEN_SIGNING_KEY`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

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
