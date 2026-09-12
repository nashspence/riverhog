# STOVE0_EXIFTOOL_OBSERVER_WORKSPACE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-exiftool-observer-workspace:ee76765edd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b5b56da79c"></a>
| Field | Shape |
|---|---|
| <a id="s-c4c426b9e4"></a>`consumers` | ["stove0-exiftool-observer"] |
| <a id="s-953e2f4a91"></a>`name` | "STOVE0_EXIFTOOL_OBSERVER_WORKSPACE" |

## Governing policies

- <a id="pa-edec5c6a90"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:STOVE0_EXIFTOOL_OBSERVER_WORKSPACE](../../../evidence/sources.md#src-2a804ca914) — `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_WORKSPACE`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/94`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 33d9b620c6c71303ae599ed1a4a49e605ef163121ef893862da038abb69dfc90 -->

```json
{
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "name": "STOVE0_EXIFTOOL_OBSERVER_WORKSPACE"
}
```
