# GOGURT_LISTENER_HOST_PROVIDER

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:gogurt-listener-host-provider:fc28a3e8f2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-7ba4daea6898"></a>
| Field | Shape |
|---|---|
| <a id="s-1fb6988e8115"></a>`consumers` | ["gogurt"] |
| <a id="s-0df8f11eac2e"></a>`name` | "GOGURT_LISTENER_HOST_PROVIDER" |

## Governing policies

- <a id="pa-a00c1de6b541"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:GOGURT_LISTENER_HOST_PROVIDER](../../../evidence/sources.md#src-640a7c2dcf8c) — `configuration-environment:GOGURT_LISTENER_HOST_PROVIDER`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/0`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f6273237e3ee8e5df7f318ac6fb4283703610b5f162da6616cee1a6f779622a9 -->

```json
{
  "consumers": [
    "gogurt"
  ],
  "name": "GOGURT_LISTENER_HOST_PROVIDER"
}
```
