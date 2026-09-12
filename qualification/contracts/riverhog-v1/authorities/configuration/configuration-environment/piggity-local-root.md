# PIGGITY_LOCAL_ROOT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:piggity-local-root:2bf9880123 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-bf86dd537b"></a>
| Field | Shape |
|---|---|
| <a id="s-98d6331adc"></a>`consumers` | ["piggity"] |
| <a id="s-ffbf567bef"></a>`name` | "PIGGITY_LOCAL_ROOT" |

## Governing policies

- <a id="pa-06a5e4a047"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:PIGGITY_LOCAL_ROOT](../../../evidence/sources.md#src-a3bab32771) — `configuration-environment:PIGGITY_LOCAL_ROOT`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/3`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 55b6221cb389005b4efa5aaf31cf3649cdb4a2b0afd8cf5b98554e72dc443175 -->

```json
{
  "consumers": [
    "piggity"
  ],
  "name": "PIGGITY_LOCAL_ROOT"
}
```
