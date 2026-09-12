# STOVE0_API_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-api-token:206c04764b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-24fc48da99"></a>
| Field | Shape |
|---|---|
| <a id="s-fa86d7a910"></a>`consumers` | ["stove0-server"] |
| <a id="s-071aa15ddd"></a>`name` | "STOVE0_API_TOKEN" |

## Governing policies

- <a id="pa-02002568c0"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:STOVE0_API_TOKEN](../../../evidence/sources.md#src-0cfef1474b) — `configuration-environment:STOVE0_API_TOKEN`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/80`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: af8acd21d4c3e7f9c303a70e1591e12d63b367802dfd20d8f17d2722953e05c7 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_API_TOKEN"
}
```
