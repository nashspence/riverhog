# STOVE0_TARGETS_JSON

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-targets-json:431130b2bd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-7baa03212b"></a>
| Field | Shape |
|---|---|
| <a id="s-8d23c42891"></a>`consumers` | ["stove0-server"] |
| <a id="s-aa9b56325e"></a>`name` | "STOVE0_TARGETS_JSON" |

## Governing policies

- <a id="pa-6d00c28b72"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:STOVE0_TARGETS_JSON](../../../evidence/sources.md#src-92e26dacb5) — `configuration-environment:STOVE0_TARGETS_JSON`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/111`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7874e6e3ef6287aab088d90e3a049674b4cf961751328e22f1033b4b2b628a90 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_TARGETS_JSON"
}
```
