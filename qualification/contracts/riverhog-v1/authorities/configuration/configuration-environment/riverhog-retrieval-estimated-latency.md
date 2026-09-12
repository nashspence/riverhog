# RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-retrieval-estimated-latency:9b58d04b3e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1c368e3bbb6d"></a>
| Field | Shape |
|---|---|
| <a id="s-0e717d7f5cbf"></a>`consumers` | ["riverhog-server"] |
| <a id="s-561f90128ca9"></a>`name` | "RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY" |

## Governing policies

- <a id="pa-2d12f81d8a9f"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY](../../../evidence/sources.md#src-cc14c4b603da) — `configuration-environment:RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/64`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 37f1a6575e0294f915e43a52e04a0133cbb80c7973d412a7e8393697207855c9 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY"
}
```
