# PIGGITY_PROVENANCE_OBSERVER

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:piggity-provenance-observer:7274288831 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-38bec3b929"></a>
| Field | Shape |
|---|---|
| <a id="s-9924516b14"></a>`consumers` | ["piggity"] |
| <a id="s-a68f317075"></a>`name` | "PIGGITY_PROVENANCE_OBSERVER" |

## Governing policies

- <a id="pa-88bf619816"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:PIGGITY_PROVENANCE_OBSERVER](../../../evidence/sources.md#src-8d51cac50a) — `configuration-environment:PIGGITY_PROVENANCE_OBSERVER`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/5`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 42759cf7f177f82d580e465305a24c36bafb9858a9b37b0780ae910274299329 -->

```json
{
  "consumers": [
    "piggity"
  ],
  "name": "PIGGITY_PROVENANCE_OBSERVER"
}
```
