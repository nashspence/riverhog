# RIVERHOG_RETRIEVAL_CACHE_STORES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-retrieval-cache-stores:d02488b900 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-51bdf3ea7314"></a>
| Field | Shape |
|---|---|
| <a id="s-46d639723248"></a>`consumers` | ["riverhog-server"] |
| <a id="s-cb037aed18f4"></a>`name` | "RIVERHOG_RETRIEVAL_CACHE_STORES" |

## Governing policies

- <a id="pa-4a7239ecd745"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:RIVERHOG_RETRIEVAL_CACHE_STORES](../../../evidence/sources.md#src-0b55d52d12ac) — `configuration-environment:RIVERHOG_RETRIEVAL_CACHE_STORES`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/60`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: de6288f0eb18ba1f868541b9a3620457d45234cfde0eef50017980b2a098be11 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_RETRIEVAL_CACHE_STORES"
}
```
