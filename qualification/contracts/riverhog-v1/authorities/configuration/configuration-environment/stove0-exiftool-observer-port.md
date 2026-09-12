# STOVE0_EXIFTOOL_OBSERVER_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-exiftool-observer-port:6d51a9fbe2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-f34a6099fd41"></a>
| Field | Shape |
|---|---|
| <a id="s-333cb6b464a5"></a>`consumers` | ["stove0-exiftool-observer"] |
| <a id="s-e8b92a27c101"></a>`name` | "STOVE0_EXIFTOOL_OBSERVER_PORT" |

## Governing policies

- <a id="pa-e4f04f1677d8"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:STOVE0_EXIFTOOL_OBSERVER_PORT](../../../evidence/sources.md#src-74ef86c74a47) — `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_PORT`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/90`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 886f07ef3a83e28268b0020c1d18bff7a0a381b272163091527354fa3ddbcbaa -->

```json
{
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "name": "STOVE0_EXIFTOOL_OBSERVER_PORT"
}
```
