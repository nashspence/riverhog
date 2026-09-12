# STOVE0_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-token:3e9aa5d575 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-03d76831f64c"></a>
| Field | Shape |
|---|---|
| <a id="s-9d176682fc3e"></a>`consumers` | ["stove0-api-client"] |
| <a id="s-709f3f0d7cf5"></a>`name` | "STOVE0_TOKEN" |

## Governing policies

- <a id="pa-a377c584423e"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:STOVE0_TOKEN](../../../evidence/sources.md#src-6341e5d86da9) — `configuration-environment:STOVE0_TOKEN`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/117`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 315d1113db111bae4c9fd5f030b87406b2d8fc1a10f20e82dd85da931029707a -->

```json
{
  "consumers": [
    "stove0-api-client"
  ],
  "name": "STOVE0_TOKEN"
}
```
