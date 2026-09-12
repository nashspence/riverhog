# STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-exiftool-observer-token-file:7b18cfb96a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1513c088ba"></a>
| Field | Shape |
|---|---|
| <a id="s-9d88b5d281"></a>`consumers` | ["stove0-exiftool-observer"] |
| <a id="s-66c149272c"></a>`name` | "STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE" |

## Governing policies

- <a id="pa-2e4f22c19d"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE](../../../evidence/sources.md#src-9aa84ad536) — `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/93`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6e71951650dd52863b47c635acdf2d7e7d6e3d57249e9bca3010be663c2895a7 -->

```json
{
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "name": "STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE"
}
```
