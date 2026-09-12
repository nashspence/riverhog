# STOVE0_WORKSPACE_ASSURANCE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-workspace-assurance:7a5e35e225 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-f0ba6e17dd"></a>
| Field | Shape |
|---|---|
| <a id="s-9c7aa095b0"></a>`consumers` | ["stove0-server"] |
| <a id="s-3e59f086de"></a>`name` | "STOVE0_WORKSPACE_ASSURANCE" |

## Governing policies

- <a id="pa-c9a996ceb3"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:STOVE0_WORKSPACE_ASSURANCE](../../../evidence/sources.md#src-c446ab90a2) — `configuration-environment:STOVE0_WORKSPACE_ASSURANCE`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/118`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e5ae80183b8904ff23d3a7ad37be1d630e4498cd8f259e6bce8523ff58582669 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_WORKSPACE_ASSURANCE"
}
```
