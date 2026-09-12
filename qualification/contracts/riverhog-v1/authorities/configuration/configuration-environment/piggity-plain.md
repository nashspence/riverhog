# PIGGITY_PLAIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:piggity-plain:d316d5221e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-e46514ac5a8d"></a>
| Field | Shape |
|---|---|
| <a id="s-0a3f486fd11a"></a>`consumers` | ["piggity"] |
| <a id="s-92c12c2ce2b0"></a>`name` | "PIGGITY_PLAIN" |

## Governing policies

- <a id="pa-802adaccd9d6"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:PIGGITY_PLAIN](../../../evidence/sources.md#src-a479b918495f) — `configuration-environment:PIGGITY_PLAIN`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/4`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 96c1a14dc58375dfd9b99106670331d30d79c0fa8c8e80a7e5b6a34f33a949c9 -->

```json
{
  "consumers": [
    "piggity"
  ],
  "name": "PIGGITY_PLAIN"
}
```
