# STOVE0_TARGET_CALLBACK_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-target-callback-base-url:fe515c8353 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-6d8ed99c25"></a>
| Field | Shape |
|---|---|
| <a id="s-455d12eaca"></a>`consumers` | ["stove0-server"] |
| <a id="s-1b616d0548"></a>`name` | "STOVE0_TARGET_CALLBACK_BASE_URL" |

## Governing policies

- <a id="pa-b0f669cd2e"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:STOVE0_TARGET_CALLBACK_BASE_URL](../../../evidence/sources.md#src-3c07f47663) — `configuration-environment:STOVE0_TARGET_CALLBACK_BASE_URL`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/114`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e6383cec2626069a73b724ada91e14edf7d0a13b588c74f5fd488d1355a6b901 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_TARGET_CALLBACK_BASE_URL"
}
```
