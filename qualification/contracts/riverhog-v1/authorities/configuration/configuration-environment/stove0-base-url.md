# STOVE0_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-base-url:15582a93e3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-205dff964b"></a>
| Field | Shape |
|---|---|
| <a id="s-668719968c"></a>`consumers` | ["stove0-api-client"] |
| <a id="s-663968b1c7"></a>`name` | "STOVE0_BASE_URL" |

## Governing policies

- <a id="pa-a44dff7691"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:STOVE0_BASE_URL](../../../evidence/sources.md#src-2df277ee96) — `configuration-environment:STOVE0_BASE_URL`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/81`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b79b44ff762fa3c233f2fcb102a9c73c0a37185639e2f4ed7de19393d1d8d782 -->

```json
{
  "consumers": [
    "stove0-api-client"
  ],
  "name": "STOVE0_BASE_URL"
}
```
