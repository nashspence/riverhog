# STOVE0_HTTP2

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-http2:19e7412ca7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-549c81fb7a"></a>
| Field | Shape |
|---|---|
| <a id="s-da01a86256"></a>`consumers` | ["stove0-api-client"] |
| <a id="s-dd46bf31d6"></a>`name` | "STOVE0_HTTP2" |

## Governing policies

- <a id="pa-c8831bc6ce"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:STOVE0_HTTP2](../../../evidence/sources.md#src-16c8d7c0b8) — `configuration-environment:STOVE0_HTTP2`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/104`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 83085736ff88a7bd34965d9b46be000a98ec5631d0346888bde2b08c421ddc40 -->

```json
{
  "consumers": [
    "stove0-api-client"
  ],
  "name": "STOVE0_HTTP2"
}
```
