# STOVE0_ALLOW_INSECURE_HTTP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-allow-insecure-http:ddda4f16d0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-d42d9004a6"></a>
| Field | Shape |
|---|---|
| <a id="s-ef9d7442ff"></a>`consumers` | ["stove0-api-client"] |
| <a id="s-c980de4ee4"></a>`name` | "STOVE0_ALLOW_INSECURE_HTTP" |

## Governing policies

- <a id="pa-4fc9b12c8f"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:STOVE0_ALLOW_INSECURE_HTTP](../../../evidence/sources.md#src-06df4fb5c3) — `configuration-environment:STOVE0_ALLOW_INSECURE_HTTP`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/79`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 840ec66382e0b1e00ee135e9bcfc95c207a44355a6498f9dc4398eae6af5e771 -->

```json
{
  "consumers": [
    "stove0-api-client"
  ],
  "name": "STOVE0_ALLOW_INSECURE_HTTP"
}
```
