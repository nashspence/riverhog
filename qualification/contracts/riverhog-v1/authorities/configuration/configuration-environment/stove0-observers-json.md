# STOVE0_OBSERVERS_JSON

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-observers-json:c8c967c775 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b00241927889"></a>
| Field | Shape |
|---|---|
| <a id="s-e3ac27b7d7cb"></a>`consumers` | ["stove0-server"] |
| <a id="s-058d8b88740d"></a>`name` | "STOVE0_OBSERVERS_JSON" |

## Governing policies

- <a id="pa-2a58a3c7bc44"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:STOVE0_OBSERVERS_JSON](../../../evidence/sources.md#src-25d3f3d25634) — `configuration-environment:STOVE0_OBSERVERS_JSON`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/107`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c87f75bb5b82ad11903b7312905f1c4038bfbd96a1e5325e42cae7926ec1f59a -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_OBSERVERS_JSON"
}
```
