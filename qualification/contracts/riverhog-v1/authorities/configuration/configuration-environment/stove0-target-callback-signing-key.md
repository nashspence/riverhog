# STOVE0_TARGET_CALLBACK_SIGNING_KEY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-target-callback-signing-key:294fd91c5d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-0cc068cac244"></a>
| Field | Shape |
|---|---|
| <a id="s-ba1e600e45e6"></a>`consumers` | ["stove0-server"] |
| <a id="s-d7bcf4391716"></a>`name` | "STOVE0_TARGET_CALLBACK_SIGNING_KEY" |

## Governing policies

- <a id="pa-9a45f7ee9815"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:STOVE0_TARGET_CALLBACK_SIGNING_KEY](../../../evidence/sources.md#src-f5bba573ec08) — `configuration-environment:STOVE0_TARGET_CALLBACK_SIGNING_KEY`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/115`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: af62be45be5635c683d4dfc5eab89364d51ff6b43fdd055a2a6a6645457e52d5 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_TARGET_CALLBACK_SIGNING_KEY"
}
```
