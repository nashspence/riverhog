# STOVE0_EXIFTOOL_OBSERVER_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-exiftool-observer-token:6b950c92dc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-651129acd1a4"></a>
| Field | Shape |
|---|---|
| <a id="s-0a8dd8ad5b34"></a>`consumers` | ["stove0-exiftool-observer"] |
| <a id="s-b54b40081760"></a>`name` | "STOVE0_EXIFTOOL_OBSERVER_TOKEN" |

## Governing policies

- <a id="pa-0281d2cc4f01"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:STOVE0_EXIFTOOL_OBSERVER_TOKEN](../../../evidence/sources.md#src-0b63b9007c19) — `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_TOKEN`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/92`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c02287675c7a36674517be8b08c2675f8a05e8557c4a0f328224959cc4611705 -->

```json
{
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "name": "STOVE0_EXIFTOOL_OBSERVER_TOKEN"
}
```
