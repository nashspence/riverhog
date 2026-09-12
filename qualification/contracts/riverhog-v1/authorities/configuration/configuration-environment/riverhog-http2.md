# RIVERHOG_HTTP2

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-http2:467ce61513 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-5581cd63e1"></a>
| Field | Shape |
|---|---|
| <a id="s-77d05a28fd"></a>`consumers` | ["riverhog-client"] |
| <a id="s-f0549843d7"></a>`name` | "RIVERHOG_HTTP2" |

## Governing policies

- <a id="pa-8ce4678ec7"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_HTTP2](../../../evidence/sources.md#src-1b5671172b) — `configuration-environment:RIVERHOG_HTTP2`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/47`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f24aae181bdcea968d17e12af08c85c8d3b998dc0fc3a9a0650d05778c8b754f -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "name": "RIVERHOG_HTTP2"
}
```
